"""业务工作负载：建档、试算、签复核，模拟生产并发写入。

每个业务动作都是 *一个* SQLite 事务，事务内同时：
1. 写业务表（与 backend/src/models/tables.py 同构）；
2. 追加一条 commit_log（等价于 binlog 中一个已提交事务的边界）。
备份快照只可能看到“整条已提交”或“完全看不到”，在途事务整笔留给后续批次。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
import uuid

from . import domain
from .common import canonical, digest_report, utc_now
from .storage import append_commit, connect, init_db, write_txn

DEMO_PASSWORD = 'Demo-Walkthrough-2026'


def hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    derived = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 200_000)
    return f'{salt.hex()}:{derived.hex()}'


def seed(db_path: str) -> dict:
    """建账号与演示档案，幂等。"""
    init_db(db_path)
    conn = connect(db_path)
    counts = {}
    try:
        with write_txn(conn) as txn:
            for role in ('operator', 'reviewer', 'viewer'):
                exists = txn.execute(
                    'SELECT id FROM accounts WHERE username=?', (role,)).fetchone()
                if exists is None:
                    cur = txn.execute(
                        'INSERT INTO accounts (username, password_hash, role)'
                        ' VALUES (?, ?, ?)',
                        (role, hash_password(DEMO_PASSWORD), role))
                    append_commit(txn, actor='system', action='seed_account',
                                  changed={'accounts': [
                                      dict(id=cur.lastrowid, username=role,
                                           password_hash='<seed>', role=role)]})
            demo = txn.execute(
                'SELECT id FROM business_cases WHERE slug=?', ('demo',)).fetchone()
            if demo is None:
                payload_text = canonical(domain.DEMO_SAMPLE).decode('utf-8')
                cur = txn.execute(
                    'INSERT INTO business_cases (slug, title, payload, author, created_at)'
                    ' VALUES (?, ?, ?, ?, ?)',
                    ('demo', domain.DEMO_SAMPLE['snapshot_id'], payload_text,
                     'demo', utc_now()))
                case_id = cur.lastrowid
                row = dict(txn.execute(
                    'SELECT * FROM business_cases WHERE id=?', (case_id,)).fetchone())
                audit_cur = txn.execute(
                    'INSERT INTO audit_events (actor, action, reference, created_at)'
                    ' VALUES (?, ?, ?, ?)',
                    ('demo', 'create_case', str(case_id), utc_now()))
                audit_row = dict(txn.execute(
                    'SELECT * FROM audit_events WHERE id=?',
                    (audit_cur.lastrowid,)).fetchone())
                append_commit(txn, actor='demo', action='create_case', changed={
                    'business_cases': [row],
                    'audit_events': [audit_row],
                })
            for table in ('accounts', 'business_cases', 'business_runs',
                          'business_evidence', 'audit_events', 'commit_log'):
                counts[table] = txn.execute(
                    f'SELECT COUNT(*) AS c FROM {table}').fetchone()['c']
    finally:
        conn.close()
    return counts


def _with_retry(func):
    def wrapped(*args, **kwargs):
        for attempt in range(50):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as exc:
                if 'locked' not in str(exc) or attempt == 49:
                    raise
                import time
                time.sleep(0.05)
    return wrapped


@_with_retry
def create_case(db_path: str, actor: str, title: str, payload: dict) -> int:
    conn = connect(db_path)
    try:
        domain.validate(payload)
        with write_txn(conn) as txn:
            cur = txn.execute(
                'INSERT INTO business_cases (slug, title, payload, author, created_at)'
                ' VALUES (?, ?, ?, ?, ?)',
                (uuid.uuid4().hex, title, canonical(payload).decode('utf-8'),
                 actor, utc_now()))
            case_id = cur.lastrowid
            case_row = dict(txn.execute(
                'SELECT * FROM business_cases WHERE id=?', (case_id,)).fetchone())
            audit_cur = txn.execute(
                'INSERT INTO audit_events (actor, action, reference, created_at)'
                ' VALUES (?, ?, ?, ?)',
                (actor, 'create_case', str(case_id), utc_now()))
            audit_row = dict(txn.execute(
                'SELECT * FROM audit_events WHERE id=?',
                (audit_cur.lastrowid,)).fetchone())
            append_commit(txn, actor=actor, action='create_case', changed={
                'business_cases': [case_row],
                'audit_events': [audit_row],
            })
        return case_id
    finally:
        conn.close()


@_with_retry
def enqueue_run(db_path: str, case_id: int, actor: str) -> int:
    conn = connect(db_path)
    try:
        with write_txn(conn) as txn:
            case = txn.execute(
                'SELECT id FROM business_cases WHERE id=?', (case_id,)).fetchone()
            if case is None:
                raise ValueError(f'档案不存在: {case_id}')
            cur = txn.execute(
                'INSERT INTO business_runs (case_id, status, owner, created_at)'
                ' VALUES (?, ?, ?, ?)',
                (case_id, 'queued', actor, utc_now()))
            run_id = cur.lastrowid
            run_row = dict(txn.execute(
                'SELECT * FROM business_runs WHERE id=?', (run_id,)).fetchone())
            audit_cur = txn.execute(
                'INSERT INTO audit_events (actor, action, reference, created_at)'
                ' VALUES (?, ?, ?, ?)',
                (actor, 'enqueue', str(run_id), utc_now()))
            audit_row = dict(txn.execute(
                'SELECT * FROM audit_events WHERE id=?',
                (audit_cur.lastrowid,)).fetchone())
            append_commit(txn, actor=actor, action='enqueue', changed={
                'business_runs': [run_row],
                'audit_events': [audit_row],
            })
        return run_id
    finally:
        conn.close()


@_with_retry
def complete_run(db_path: str, run_id: int) -> int:
    """试算完成：run 置 complete 并写证据，单事务。"""
    conn = connect(db_path)
    try:
        with write_txn(conn) as txn:
            run = txn.execute(
                'SELECT * FROM business_runs WHERE id=?', (run_id,)).fetchone()
            if run is None:
                raise ValueError(f'试算不存在: {run_id}')
            case = txn.execute(
                'SELECT * FROM business_cases WHERE id=?',
                (run['case_id'],)).fetchone()
            payload = json.loads(case['payload'])
            report = domain.solve(payload)
            digest = digest_report(report)
            txn.execute(
                'UPDATE business_runs SET status=?, finished_at=? WHERE id=?',
                ('complete', utc_now(), run_id))
            run_row = dict(txn.execute(
                'SELECT * FROM business_runs WHERE id=?', (run_id,)).fetchone())
            ev_cur = txn.execute(
                'INSERT INTO business_evidence (run_id, report, digest)'
                ' VALUES (?, ?, ?)',
                (run_id, canonical(report).decode('utf-8'), digest))
            evidence_id = ev_cur.lastrowid
            ev_row = dict(txn.execute(
                'SELECT * FROM business_evidence WHERE id=?',
                (evidence_id,)).fetchone())
            audit_cur = txn.execute(
                'INSERT INTO audit_events (actor, action, reference, created_at)'
                ' VALUES (?, ?, ?, ?)',
                ('worker', 'complete', str(run_id), utc_now()))
            audit_row = dict(txn.execute(
                'SELECT * FROM audit_events WHERE id=?',
                (audit_cur.lastrowid,)).fetchone())
            append_commit(txn, actor='worker', action='complete', changed={
                'business_runs': [run_row],
                'business_evidence': [ev_row],
                'audit_events': [audit_row],
            })
        return evidence_id
    finally:
        conn.close()


@_with_retry
def review_evidence(db_path: str, evidence_id: int, note: str, actor: str) -> None:
    conn = connect(db_path)
    try:
        with write_txn(conn) as txn:
            ev = txn.execute(
                'SELECT * FROM business_evidence WHERE id=?',
                (evidence_id,)).fetchone()
            if ev is None:
                raise ValueError(f'证据不存在: {evidence_id}')
            if ev['reviewed_at'] is not None:
                raise ValueError(f'证据 {evidence_id} 已复核，不可重复签署')
            txn.execute(
                'UPDATE business_evidence SET reviewer=?, review_note=?, reviewed_at=?'
                ' WHERE id=?',
                (actor, note, utc_now(), evidence_id))
            ev_row = dict(txn.execute(
                'SELECT * FROM business_evidence WHERE id=?',
                (evidence_id,)).fetchone())
            audit_cur = txn.execute(
                'INSERT INTO audit_events (actor, action, reference, created_at)'
                ' VALUES (?, ?, ?, ?)',
                (actor, 'review', str(evidence_id), utc_now()))
            audit_row = dict(txn.execute(
                'SELECT * FROM audit_events WHERE id=?',
                (audit_cur.lastrowid,)).fetchone())
            append_commit(txn, actor=actor, action='review', changed={
                'business_evidence': [ev_row],
                'audit_events': [audit_row],
            })
    finally:
        conn.close()


# ---- 显式长事务：演示“在途事务整笔留给后续批次” -----------------------------

class InFlightCase(threading.local):
    pass


def begin_case_txn(db_path: str, actor: str, title: str, payload: dict):
    """打开一个跨越备份窗口的写事务，提交前对任何快照都不可见。"""
    domain.validate(payload)
    conn = connect(db_path)
    conn.execute('BEGIN IMMEDIATE')
    cur = conn.execute(
        'INSERT INTO business_cases (slug, title, payload, author, created_at)'
        ' VALUES (?, ?, ?, ?, ?)',
        (uuid.uuid4().hex, title, canonical(payload).decode('utf-8'),
         actor, utc_now()))
    case_id = cur.lastrowid
    conn.execute(
        'INSERT INTO audit_events (actor, action, reference, created_at)'
        ' VALUES (?, ?, ?, ?)',
        (actor, 'create_case', str(case_id), utc_now()))
    return conn, case_id


def commit_case_txn(conn: sqlite3.Connection, case_id: int, actor: str) -> int:
    try:
        case_row = dict(conn.execute(
            'SELECT * FROM business_cases WHERE id=?', (case_id,)).fetchone())
        audit_row = dict(conn.execute(
            'SELECT * FROM audit_events WHERE reference=? AND action=?',
            (str(case_id), 'create_case')).fetchone())
        seq = append_commit(conn, actor=actor, action='create_case', changed={
            'business_cases': [case_row],
            'audit_events': [audit_row],
        })
        conn.commit()
        return seq
    finally:
        conn.close()


def rollback_case_txn(conn: sqlite3.Connection) -> None:
    try:
        conn.rollback()
    finally:
        conn.close()
