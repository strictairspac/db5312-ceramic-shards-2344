"""SQLite 存储层，模拟生产 MySQL 库（workflow 库的五张业务表 + 审计）。

生产对应关系
------------
- 业务表与 backend/src/models/tables.py 同名：
  accounts / business_cases / business_runs / business_evidence / audit_events。
- 每个写动作在 *同一个事务* 内同时追加一条 commit_log（对应生产的 binlog 事件
  边界）：一个事务要么整条可见，要么不可见，天然满足“同一提交边界”。
- 备份全程只对源库发起 BEGIN IMMEDIATE 快照读，绝不执行写语句；备份工具持有的
  连接以只读模式打开，代码层面杜绝误写源库。
- 生产 MySQL 适配点：
  * 全量 = mysqldump --single-transaction --master-data，记录 binlog 位点；
  * 增量 = mysqlbinlog --stop-never 按位点切批，每批带起止位点与 SHA-256；
  * 本演练用 commit_log 的 seq 替代 binlog 位点，语义同为单调递增的提交序号。
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import contextmanager

from .common import utc_now

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS business_cases (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    payload TEXT NOT NULL,
    author TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS business_runs (
    id INTEGER PRIMARY KEY,
    case_id INTEGER NOT NULL REFERENCES business_cases(id),
    status TEXT NOT NULL,
    owner TEXT NOT NULL,
    attempt_token TEXT,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_case ON business_runs(case_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON business_runs(status);
CREATE TABLE IF NOT EXISTS business_evidence (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL UNIQUE REFERENCES business_runs(id),
    report TEXT NOT NULL,
    digest TEXT NOT NULL,
    reviewer TEXT,
    review_note TEXT,
    reviewed_at TEXT
);
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    reference TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- 提交日志：一次业务事务恰好一条，等价于 binlog 中的一个已提交事务。
CREATE TABLE IF NOT EXISTS commit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    tables_json TEXT NOT NULL,
    rows_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS drill_bookkeeping (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""

TABLES = ('accounts', 'business_cases', 'business_runs',
          'business_evidence', 'audit_events', 'commit_log')
# 恢复完整性校验只覆盖业务表；commit_log 是重放依据本身。
BUSINESS_TABLES = ('accounts', 'business_cases', 'business_runs',
                   'business_evidence', 'audit_events')
# 全量转储必须包含提交日志，恢复后位点才能从 1 连续。
FULL_DUMP_TABLES = BUSINESS_TABLES + ('commit_log',)


def connect(db_path: str, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = f'file:{os.path.abspath(db_path)}?mode=ro'
        conn = sqlite3.connect(uri, uri=True, timeout=30)
    else:
        conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """业务写事务：返回游标，异常回滚，成功提交。"""
    try:
        conn.execute('BEGIN IMMEDIATE')
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def max_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute('SELECT COALESCE(MAX(seq), 0) AS s FROM commit_log').fetchone()
    return int(row['s'])


def append_commit(conn: sqlite3.Connection, *, actor: str, action: str,
                  changed: dict[str, list[dict]]) -> int:
    """在当前事务内追加提交日志，返回新的 seq。

    changed: {表名: [行字典, ...]}，行字典必须包含主键 id。
    演练的写入面只有 INSERT/UPDATE，删除不在生产业务范围内。
    """
    from .common import canonical, sha256_bytes

    txn_id = uuid.uuid4().hex
    payload = {'txn_id': txn_id, 'actor': actor, 'action': action,
               'changed': {t: changed[t] for t in sorted(changed)}}
    checksum = sha256_bytes(canonical(payload))
    cur = conn.execute(
        'INSERT INTO commit_log (txn_id, actor, action, tables_json, rows_json,'
        ' checksum, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
        (txn_id, actor, action,
         canonical(sorted(changed.keys())).decode('utf-8'),
         canonical({t: changed[t] for t in sorted(changed)}).decode('utf-8'),
         checksum, utc_now()))
    return int(cur.lastrowid)


def fetch_commits(conn: sqlite3.Connection, *, after_seq: int,
                  limit: int | None = None) -> list[sqlite3.Row]:
    sql = 'SELECT * FROM commit_log WHERE seq > ? ORDER BY seq'
    params: list = [after_seq]
    if limit is not None:
        sql += ' LIMIT ?'
        params.append(limit)
    return list(conn.execute(sql, params).fetchall())


def table_counts(conn: sqlite3.Connection, tables=BUSINESS_TABLES) -> dict[str, int]:
    return {t: conn.execute(f'SELECT COUNT(*) AS c FROM {t}').fetchone()['c']
            for t in tables}
