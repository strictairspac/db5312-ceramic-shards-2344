"""备份：首份全量，其后只留增量。

批次目录结构（位于仓库目录下）::

    repo/
      catalog.json                 # 批次清单（原子写），登记全部批次与位点链
      full-0001/
        manifest.json              # 批次元数据（位点、文件 SHA-256、表行数）
        snapshot.sqlite            # 一致性快照（在线 backup API，对应 mysqldump 单事务）
        rows/accounts.jsonl ...    # 逻辑行转储，逐行哈希，供恢复与逐表核对
      incr-0002/
        manifest.json
        commits/000007.jsonl ...   # 提交日志片段（对应按 binlog 位点切出的增量）

关键保证
--------
- 源库以 ``mode=ro`` 打开，备份工具物理上无法写源库（“全程不写源库”）。
- 全量用 SQLite 在线 backup API，在单一一致性快照下完成；并发写事务要么整条
  在内、要么整条在外，不存在跨边界半条事务。
- 每个增量批次记录 prev_end/end 位点（commit_log.seq，对应 binlog 位点），
  恢复时按链核对，缺口即停。
"""
from __future__ import annotations

import os
import sqlite3

from .common import (canonical, ensure_dir, read_json, sha256_bytes,
                     sha256_file, utc_now, write_json_atomic)
from .storage import (BUSINESS_TABLES, FULL_DUMP_TABLES, connect,
                      fetch_commits, max_seq)

CATALOG_NAME = 'catalog.json'
FULL_PREFIX = 'full-'
INCR_PREFIX = 'incr-'


# ---- 批次目录与清单 ---------------------------------------------------------

def catalog_path(repo: str) -> str:
    return os.path.join(repo, CATALOG_NAME)


def load_catalog(repo: str) -> dict:
    path = catalog_path(repo)
    if not os.path.exists(path):
        return {'batches': []}
    return read_json(path)


def save_catalog(repo: str, catalog: dict) -> None:
    ensure_dir(repo)
    write_json_atomic(catalog_path(repo), catalog)


def batch_dir(repo: str, batch_id: str) -> str:
    return os.path.join(repo, batch_id)


def _next_batch_no(catalog: dict) -> int:
    if not catalog['batches']:
        return 1
    return max(b['batch_no'] for b in catalog['batches']) + 1


def _file_meta(path: str) -> dict:
    return {'path': os.path.basename(path), 'sha256': sha256_file(path),
            'bytes': os.path.getsize(path)}


# ---- 全量备份 ---------------------------------------------------------------

def _dump_rows(snapshot_conn: sqlite3.Connection, out_dir: str,
               tables: tuple[str, ...]) -> tuple[dict, dict]:
    """从一致性快照连接导出逻辑行，保证行转储与 snapshot.sqlite 同一时点。"""
    rows_dir = ensure_dir(os.path.join(out_dir, 'rows'))
    files, counts = {}, {}
    for table in tables:
        order_col = 'seq' if table == 'commit_log' else 'id'
        rows = snapshot_conn.execute(
            f'SELECT * FROM {table} ORDER BY {order_col}').fetchall()
        lines = []
        for row in rows:
            row_dict = dict(row)
            line = canonical(row_dict).decode('utf-8')
            lines.append(line)
        path = os.path.join(rows_dir, f'{table}.jsonl')
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write('\n'.join(lines))
            if lines:
                handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        files[f'rows/{table}.jsonl'] = _file_meta(path)
        counts[table] = len(rows)
    return files, counts


def create_full(repo: str, source_db: str, *, note: str = '') -> dict:
    catalog = load_catalog(repo)
    if catalog['batches']:
        raise RuntimeError('已存在全量批次；后续只能追加增量（演练策略：首份之后只留增量）')
    batch_no = 1
    batch_id = f'{FULL_PREFIX}{batch_no:04d}'
    out_dir = ensure_dir(batch_dir(repo, batch_id))

    ro = connect(source_db, readonly=True)
    snapshot_path = os.path.join(out_dir, 'snapshot.sqlite')
    try:
        # 在线 backup：单一一致性快照，备份期间并发业务照常提交且互不阻塞。
        target = sqlite3.connect(snapshot_path)
        try:
            ro.backup(target)
        finally:
            target.close()
        # 行转储、位点与行数全部从快照文件读取，四者严格同一时点。
        snap_conn = sqlite3.connect(snapshot_path)
        snap_conn.row_factory = sqlite3.Row
        try:
            end_seq = max_seq(snap_conn)
            files, counts = _dump_rows(snap_conn, out_dir, FULL_DUMP_TABLES)
        finally:
            snap_conn.close()
    finally:
        ro.close()

    files['snapshot.sqlite'] = _file_meta(snapshot_path)
    manifest = {
        'batch_id': batch_id,
        'batch_no': batch_no,
        'kind': 'full',
        'created_at': utc_now(),
        'note': note,
        'source_db': os.path.abspath(source_db),
        'prev_end_seq': 0,
        'end_seq': end_seq,
        'table_counts': counts,
        'files': files,
    }
    write_json_atomic(os.path.join(out_dir, 'manifest.json'), manifest)
    catalog['batches'].append({
        'batch_id': batch_id, 'batch_no': batch_no, 'kind': 'full',
        'prev_end_seq': 0, 'end_seq': end_seq,
        'created_at': manifest['created_at'], 'note': note})
    save_catalog(repo, catalog)
    return manifest


# ---- 增量备份 ---------------------------------------------------------------

def create_incr(repo: str, source_db: str, *, note: str = '') -> dict:
    catalog = load_catalog(repo)
    if not catalog['batches']:
        raise RuntimeError('缺少全量基线，无法创建增量批次')
    prev = catalog['batches'][-1]
    prev_end = prev['end_seq']
    batch_no = _next_batch_no(catalog)
    batch_id = f'{INCR_PREFIX}{batch_no:04d}'
    out_dir = ensure_dir(batch_dir(repo, batch_id))
    commits_dir = ensure_dir(os.path.join(out_dir, 'commits'))

    ro = connect(source_db, readonly=True)
    try:
        # 显式只读事务：提交片段与源库计数取自同一一致性快照。
        ro.execute('BEGIN')
        commits = fetch_commits(ro, after_seq=prev_end)
        if not commits:
            ro.rollback()
            os.rmdir(commits_dir)
            os.rmdir(out_dir)
            raise RuntimeError(f'位点 {prev_end} 之后没有新的已提交事务，不生成空增量批次')
        end_seq = int(commits[-1]['seq'])
        # 位点连续性：源库自上次备份以来的提交必须连续可取（缺口在恢复侧再做链校验）。
        expected = list(range(prev_end + 1, end_seq + 1))
        actual = [int(c['seq']) for c in commits]
        if actual != expected:
            ro.rollback()
            raise RuntimeError(f'源库提交序列与上次位点不连续：期望 {expected}，实际 {actual}')

        fragment = os.path.join(commits_dir, f'{prev_end + 1:09d}-{end_seq:09d}.jsonl')
        per_commit = []
        with open(fragment, 'w', encoding='utf-8') as handle:
            for commit in commits:
                row = dict(commit)
                line = canonical(row).decode('utf-8')
                handle.write(line + '\n')
                per_commit.append({'seq': row['seq'], 'txn_id': row['txn_id'],
                                   'action': row['action'],
                                   'sha256': sha256_bytes(line.encode('utf-8'))})
            handle.flush()
            os.fsync(handle.fileno())
        counts = {}
        for table in BUSINESS_TABLES:
            counts[table] = ro.execute(
                f'SELECT COUNT(*) AS c FROM {table}').fetchone()['c']
        ro.rollback()
    finally:
        ro.close()

    files = {f'commits/{os.path.basename(fragment)}': _file_meta(fragment)}
    manifest = {
        'batch_id': batch_id,
        'batch_no': batch_no,
        'kind': 'incremental',
        'created_at': utc_now(),
        'note': note,
        'source_db': os.path.abspath(source_db),
        'prev_end_seq': prev_end,
        'end_seq': end_seq,
        'commit_count': len(per_commit),
        'commits': per_commit,
        'source_table_counts_at_end': counts,
        'files': files,
    }
    write_json_atomic(os.path.join(out_dir, 'manifest.json'), manifest)
    catalog['batches'].append({
        'batch_id': batch_id, 'batch_no': batch_no, 'kind': 'incremental',
        'prev_end_seq': prev_end, 'end_seq': end_seq,
        'created_at': manifest['created_at'], 'note': note})
    save_catalog(repo, catalog)
    return manifest


def create_auto(repo: str, source_db: str, *, note: str = '') -> dict:
    """无全量则全量，否则增量。"""
    catalog = load_catalog(repo)
    if not catalog['batches']:
        return create_full(repo, source_db, note=note)
    return create_incr(repo, source_db, note=note)
