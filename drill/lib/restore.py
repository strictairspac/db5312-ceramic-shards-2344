"""恢复：按选定备份批次重放到隔离库，逐笔校验，验收通过才交付。

流程
----
1. 从全量基线开始，沿 catalog 位点链逐批校验：
   - 批次文件 SHA-256 与 manifest 不符 → 停止；
   - prev_end_seq 与上一批 end_seq 对不上（增量缺口）→ 停止；
   - 增量逐行 SHA-256 不符 → 停止。
2. 在隔离区新建版本库（restores/restore-<批次>-attemptN/），全量快照直接
   落库，增量按 commit_log 顺序逐事务重放业务表与提交日志。
3. 重放后做完整性校验（行数、外键、证据校验码、run/evidence 配对、审计边界）。
4. 全部通过才把版本登记为 accepted；失败只留 failed 记录并可重试，
   重试生成新版本号，绝不覆盖已验收版本。
5. 恢复过程只打开备份仓库与隔离区库，不连接源库（代码无 source 句柄）。
"""
from __future__ import annotations

import json
import os
import sqlite3

from .common import (digest_report, ensure_dir, read_json,
                     sha256_file, utc_now, write_json_atomic)
from .storage import BUSINESS_TABLES, FULL_DUMP_TABLES, connect, init_db
from . import backup

REGISTRY_NAME = 'registry.json'
RESTORES_DIR = 'restores'


class RestoreChainError(RuntimeError):
    """增量缺口或校验不符：按规程必须立即停止。"""


class RestoreVerificationError(RuntimeError):
    """恢复后完整性校验未通过。"""


# ---- 隔离区与版本登记 -------------------------------------------------------

def restores_root(zone: str) -> str:
    return ensure_dir(os.path.join(zone, RESTORES_DIR))


def registry_path(zone: str) -> str:
    return os.path.join(zone, REGISTRY_NAME)


def load_registry(zone: str) -> dict:
    path = registry_path(zone)
    if not os.path.exists(path):
        return {'accepted_version': None, 'attempts': []}
    return read_json(path)


def save_registry(zone: str, registry: dict) -> None:
    ensure_dir(zone)
    write_json_atomic(registry_path(zone), registry)


def accepted_path(zone: str) -> str | None:
    registry = load_registry(zone)
    version = registry.get('accepted_version')
    if not version:
        return None
    return os.path.join(restores_root(zone), version, 'workflow.sqlite')


# ---- 备份链校验 -------------------------------------------------------------

def _verify_file(repo: str, batch_dir_name: str, rel_path: str, meta: dict) -> None:
    full = os.path.join(repo, batch_dir_name, rel_path)
    if not os.path.exists(full):
        raise RestoreChainError(f'备份文件缺失：{batch_dir_name}/{rel_path}')
    actual = sha256_file(full)
    if actual != meta['sha256']:
        raise RestoreChainError(
            f'校验不符：{batch_dir_name}/{rel_path} 期望 {meta["sha256"][:16]}… '
            f'实际 {actual[:16]}…')
    if os.path.getsize(full) != meta['bytes']:
        raise RestoreChainError(f'文件大小不符：{batch_dir_name}/{rel_path}')


def verify_chain(repo: str, target_batch_id: str) -> list[dict]:
    """校验并返回到目标批次（含）为止的全部 manifest，按批次顺序。"""
    import sqlite3
    catalog = backup.load_catalog(repo)
    ids = [b['batch_id'] for b in catalog['batches']]
    if target_batch_id not in ids:
        raise RestoreChainError(f'备份仓库中找不到批次 {target_batch_id}')
    selected = catalog['batches'][:ids.index(target_batch_id) + 1]
    if selected[0]['kind'] != 'full':
        raise RestoreChainError('备份链必须以全量批次开头')

    manifests, prev_end = [], 0
    for entry in selected:
        bdir = entry['batch_id']
        manifest_path = os.path.join(backup.batch_dir(repo, bdir), 'manifest.json')
        if not os.path.exists(manifest_path):
            raise RestoreChainError(f'批次清单缺失：{bdir}/manifest.json')
        manifest = read_json(manifest_path)
        if manifest['prev_end_seq'] != prev_end:
            raise RestoreChainError(
                f'增量缺口：{bdir} 起点 {manifest["prev_end_seq"]} '
                f'与上一批终点 {prev_end} 不连续，停止恢复')
        for rel_path, meta in manifest['files'].items():
            _verify_file(repo, bdir, rel_path, meta)
        if manifest['kind'] == 'full':
            # 快照内部位点必须与清单一致，防止快照与行转储被调换。
            snap = sqlite3.connect(
                os.path.join(repo, bdir, 'snapshot.sqlite'))
            try:
                snap_end = snap.execute(
                    'SELECT COALESCE(MAX(seq),0) FROM commit_log').fetchone()[0]
            finally:
                snap.close()
            if snap_end != manifest['end_seq']:
                raise RestoreChainError(
                    f'{bdir} 快照位点 {snap_end} 与清单 {manifest["end_seq"]} 不符')
        if manifest['kind'] == 'incremental':
            _verify_commit_fragment(repo, bdir, manifest)
        manifests.append(manifest)
        prev_end = manifest['end_seq']
    return manifests


def _verify_commit_fragment(repo: str, bdir: str, manifest: dict) -> None:
    fragment_rel = next(k for k in manifest['files'] if k.startswith('commits/'))
    fragment = os.path.join(repo, bdir, fragment_rel)
    expected = {c['seq']: c for c in manifest['commits']}
    seen = []
    with open(fragment, 'r', encoding='utf-8') as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.rstrip('\n')
            if not line:
                continue
            row = json.loads(line)
            seq = row['seq']
            if seq not in expected:
                raise RestoreChainError(f'{bdir} 出现清单外提交 seq={seq}')
            from .common import sha256_bytes
            if sha256_bytes(line.encode('utf-8')) != expected[seq]['sha256']:
                raise RestoreChainError(f'{bdir} 第 {line_no} 行提交校验不符 seq={seq}')
            seen.append(seq)
    if sorted(seen) != sorted(expected):
        raise RestoreChainError(f'{bdir} 提交条目与清单不一致：{seen}')
    if seen != list(range(manifest['prev_end_seq'] + 1, manifest['end_seq'] + 1)):
        raise RestoreChainError(f'{bdir} 提交位点不连续，疑似增量缺口')


# ---- 重放 -------------------------------------------------------------------

def _load_rows_from_full(repo: str, full_manifest: dict) -> dict[str, list[dict]]:
    bdir = full_manifest['batch_id']
    data = {}
    for table in FULL_DUMP_TABLES:
        rel = f'rows/{table}.jsonl'
        path = os.path.join(repo, bdir, rel)
        rows = []
        with open(path, 'r', encoding='utf-8') as handle:
            for line in handle:
                line = line.rstrip('\n')
                if line:
                    rows.append(json.loads(line))
        if len(rows) != full_manifest['table_counts'][table]:
            raise RestoreVerificationError(
                f'全量 {table} 行数 {len(rows)} 与清单 {full_manifest["table_counts"][table]} 不符')
        data[table] = rows
    return data


def _insert_rows(conn: sqlite3.Connection, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ','.join('?' for _ in cols)
    conn.executemany(
        f'INSERT INTO {table} ({",".join(cols)}) VALUES ({placeholders})',
        [tuple(row[c] for c in cols) for row in rows])


def _replay_commit(conn: sqlite3.Connection, commit: dict) -> None:
    """按一笔提交的变更重放业务表，并原样登记 commit_log。"""
    changed = json.loads(commit['rows_json'])
    for table, rows in changed.items():
        if table not in BUSINESS_TABLES:
            raise RestoreVerificationError(f'提交 seq={commit["seq"]} 含未知表 {table}')
        for row in rows:
            existing = conn.execute(
                f'SELECT id FROM {table} WHERE id=?', (row['id'],)).fetchone()
            if existing is None:
                _insert_rows(conn, table, [row])
            else:
                cols = [c for c in row.keys() if c != 'id']
                assignments = ', '.join(f'{c}=?' for c in cols)
                conn.execute(
                    f'UPDATE {table} SET {assignments} WHERE id=?',
                    tuple(row[c] for c in cols) + (row['id'],))
    conn.execute(
        'INSERT INTO commit_log (seq, txn_id, actor, action, tables_json,'
        ' rows_json, checksum, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (commit['seq'], commit['txn_id'], commit['actor'], commit['action'],
         commit['tables_json'], commit['rows_json'], commit['checksum'],
         commit['created_at']))


def _read_fragment(repo: str, manifest: dict) -> list[dict]:
    fragment_rel = next(k for k in manifest['files'] if k.startswith('commits/'))
    path = os.path.join(repo, manifest['batch_id'], fragment_rel)
    commits = []
    with open(path, 'r', encoding='utf-8') as handle:
        for line in handle:
            line = line.rstrip('\n')
            if line:
                commits.append(json.loads(line))
    return commits


# ---- 完整性校验 -------------------------------------------------------------

def verify_database(db_path: str, expected_end_seq: int,
                    expected_counts: dict[str, int] | None = None) -> dict:
    conn = connect(db_path)
    findings = []
    try:
        fk_violations = conn.execute('PRAGMA foreign_key_check').fetchall()
        if fk_violations:
            findings.append(f'外键违规 {len(fk_violations)} 条')

        end_seq = conn.execute(
            'SELECT COALESCE(MAX(seq),0) AS s FROM commit_log').fetchone()['s']
        if end_seq != expected_end_seq:
            findings.append(f'提交终点 {end_seq} 与期望 {expected_end_seq} 不符')
        seqs = [r['seq'] for r in conn.execute(
            'SELECT seq FROM commit_log ORDER BY seq').fetchall()]
        if seqs != list(range(1, expected_end_seq + 1)):
            findings.append('commit_log 位点不连续，存在缺口')

        # 证据校验码：报告规范化 JSON 的 SHA-256 必须与 digest 一致。
        bad_digest = []
        for ev in conn.execute('SELECT id, report, digest FROM business_evidence'):
            report = json.loads(ev['report'])
            if digest_report(report) != ev['digest']:
                bad_digest.append(ev['id'])
        if bad_digest:
            findings.append(f'证据校验码不符：{bad_digest}')

        # 档案/试算/报告的提交边界配对：complete 必有证据，证据必属 complete。
        orphans = conn.execute(
            'SELECT e.id FROM business_evidence e JOIN business_runs r ON r.id=e.run_id'
            ' WHERE r.status != "complete"').fetchall()
        if orphans:
            findings.append(f'存在非 complete 试算的证据：{[o["id"] for o in orphans]}')
        complete_without_evidence = conn.execute(
            'SELECT r.id FROM business_runs r LEFT JOIN business_evidence e ON e.run_id=r.id'
            ' WHERE r.status="complete" AND e.id IS NULL').fetchall()
        if complete_without_evidence:
            findings.append(
                f'complete 试算缺少证据：{[r["id"] for r in complete_without_evidence]}')

        # 复核边界：已复核证据三字段齐备，未复核三字段皆空。
        bad_review = conn.execute(
            'SELECT id FROM business_evidence WHERE'
            ' (reviewed_at IS NULL AND (reviewer IS NOT NULL OR review_note IS NOT NULL))'
            ' OR (reviewed_at IS NOT NULL AND (reviewer IS NULL OR review_note IS NULL))'
        ).fetchall()
        if bad_review:
            findings.append(f'复核签署边界不完整：{[r["id"] for r in bad_review]}')

        counts = {t: conn.execute(
            f'SELECT COUNT(*) AS c FROM {t}').fetchone()['c']
            for t in BUSINESS_TABLES}
        if expected_counts and counts != expected_counts:
            findings.append(f'表行数 {counts} 与期望 {expected_counts} 不符')
    finally:
        conn.close()
    if findings:
        raise RestoreVerificationError('；'.join(findings))
    return {'end_seq': end_seq, 'table_counts': counts}


def _expected_counts(manifests: list[dict]) -> dict[str, int]:
    """期望行数直接取自最后一个批次清单记录的源库同快照计数。

    UPDATE（如 complete/review）不改变行数，按变更笔数推导会多算；
    备份侧在同一只读事务内取计数，恢复侧以该权威值核对。
    """
    last = manifests[-1]
    if last['kind'] == 'full':
        return {t: last['table_counts'][t] for t in BUSINESS_TABLES}
    return dict(last['source_table_counts_at_end'])


# ---- 恢复入口 ---------------------------------------------------------------

def restore(repo: str, zone: str, target_batch_id: str, *,
            note: str = '', fail_on_attempt: int | None = None,
            fail_target_attempt: int | None = None) -> dict:
    """恢复到指定批次。

    fail_on_attempt: 在全局第 N 次尝试时注入写入前中断（演练重试）。
    fail_target_attempt: 在本批次的第 N 次尝试时注入（与目标批次解耦时使用）。
    """
    registry = load_registry(zone)
    attempt_no = len(registry['attempts']) + 1
    target_attempt_no = sum(
        1 for a in registry['attempts']
        if a['target_batch'] == target_batch_id) + 1
    version = f'restore-{target_batch_id}-attempt{attempt_no:02d}'
    version_dir = ensure_dir(os.path.join(restores_root(zone), version))
    db_path = os.path.join(version_dir, 'workflow.sqlite')
    result = {
        'version': version, 'attempt': attempt_no, 'target_batch': target_batch_id,
        'started_at': utc_now(), 'finished_at': None, 'status': 'failed',
        'note': note, 'chain_verified': [], 'replayed_commits': 0,
        'verification': None, 'error': None,
    }

    try:
        # 1) 备份链校验：缺口/校验不符立即停止，不写隔离库业务数据。
        manifests = verify_chain(repo, target_batch_id)
        result['chain_verified'] = [m['batch_id'] for m in manifests]

        # 演练注入：模拟恢复中断（写入前），证明可重试且不污染已验收版本。
        should_fail = (fail_on_attempt == attempt_no
                       or fail_target_attempt == target_attempt_no)
        if should_fail:
            raise RuntimeError('注入故障：恢复写入前中断（演练）')

        # 2) 全量基线落隔离库（含 commit_log，保证位点从 1 连续）。
        init_db(db_path)
        full_rows = _load_rows_from_full(repo, manifests[0])
        conn = connect(db_path)
        try:
            with conn:
                for table in FULL_DUMP_TABLES:
                    _insert_rows(conn, table, full_rows[table])
        finally:
            conn.close()

        # 3) 增量逐事务重放（每笔一个事务，保持提交边界）。
        replayed = 0
        for manifest in manifests[1:]:
            conn = connect(db_path)
            try:
                for commit in _read_fragment(repo, manifest):
                    with conn:
                        _replay_commit(conn, commit)
                    replayed += 1
            finally:
                conn.close()
        result['replayed_commits'] = replayed

        # 4) 完整性校验。
        expected_counts = _expected_counts(manifests)
        verification = verify_database(
            db_path, manifests[-1]['end_seq'], expected_counts)
        result['verification'] = verification
        result['status'] = 'accepted'
    except (RestoreChainError, RestoreVerificationError, RuntimeError,
            sqlite3.Error, OSError, json.JSONDecodeError) as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
        result['status'] = 'failed'

    result['finished_at'] = utc_now()
    write_json_atomic(os.path.join(version_dir, 'result.json'), result)

    # 5) 登记：失败可重试；成功才更新 accepted，且已验收版本目录永不覆盖。
    registry['attempts'].append({
        'version': version, 'target_batch': target_batch_id,
        'status': result['status'], 'started_at': result['started_at'],
        'finished_at': result['finished_at'], 'error': result['error']})
    if result['status'] == 'accepted':
        previous = registry.get('accepted_version')
        if previous and previous != version:
            prev_path = os.path.join(restores_root(zone), previous)
            if not os.path.isdir(prev_path):
                raise RuntimeError('已验收版本目录缺失，拒绝切换')
        registry['accepted_version'] = version
        registry['accepted_at'] = result['finished_at']
    save_registry(zone, registry)
    return result


def query_accepted(zone: str) -> dict:
    """上一份验收通过的演练库照常可查：只读打开并返回摘要。"""
    path = accepted_path(zone)
    if not path:
        return {'available': False}
    conn = connect(path, readonly=True)
    try:
        summary = {
            'available': True,
            'db_path': os.path.abspath(path),
            'accepted_version': load_registry(zone)['accepted_version'],
            'table_counts': {t: conn.execute(
                f'SELECT COUNT(*) AS c FROM {t}').fetchone()['c']
                for t in BUSINESS_TABLES},
            'latest_cases': [dict(r) for r in conn.execute(
                'SELECT id, slug, title, author, created_at FROM business_cases'
                ' ORDER BY id DESC LIMIT 5')],
            'latest_runs': [dict(r) for r in conn.execute(
                'SELECT id, case_id, status, owner, finished_at FROM business_runs'
                ' ORDER BY id DESC LIMIT 5')],
            'latest_reviews': [dict(r) for r in conn.execute(
                'SELECT id, run_id, reviewer, reviewed_at FROM business_evidence'
                ' WHERE reviewed_at IS NOT NULL ORDER BY id DESC LIMIT 5')],
        }
    finally:
        conn.close()
    return summary
