"""并发业务负载：备份期间照常建档、完成试算、签复核。

工作线程各自打开独立连接（SQLite WAL 允许备份快照与并发写同时进行）。
另提供一个显式长事务，跨越备份窗口保持在途，验证“在途事务整笔留给后续批次”。
"""
from __future__ import annotations

import copy
import threading
import time

from . import domain, workload
from .common import utc_now
from .storage import connect, max_seq


def _variant_payload(seq_no: int) -> dict:
    payload = copy.deepcopy(domain.DEMO_SAMPLE)
    payload['snapshot_id'] = f'TY-DRILL-{seq_no:06d}'
    # 微调预算/占用，保证仍是合法盘点且报告内容各不相同。
    payload['round_budget_bytes'] = 120_000_000 + (seq_no % 5) * 5_000_000
    payload['batches'][0] = dict(payload['batches'][0])
    payload['batches'][0]['size_bytes'] = 30_000_000 + (seq_no % 4) * 1_000_000
    domain.validate(payload)
    return payload


class ConcurrentLoad:
    """后台并发负载，stop() 后线程全部收口。"""

    def __init__(self, db_path: str, *, scale: int = 6, prefix: str = 'DRILL'):
        self.db_path = db_path
        self.scale = scale
        self.prefix = prefix
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.lock = threading.Lock()
        self.created_cases = 0
        self.completed_runs = 0
        self.reviews = 0
        self.errors: list[str] = []
        self._counter = 0
        self._pending_reviews: list[int] = []

    def _next_seq(self) -> int:
        with self.lock:
            self._counter += 1
            return self._counter

    def _operator_loop(self, name: str):
        while not self._stop.is_set():
            seq_no = self._next_seq()
            try:
                case_id = workload.create_case(
                    self.db_path, 'operator',
                    f'{self.prefix} 夜班档案 {seq_no}',
                    _variant_payload(seq_no))
                run_id = workload.enqueue_run(self.db_path, case_id, 'operator')
                with self.lock:
                    self.created_cases += 1
                    self._pending_reviews.append(run_id)
            except Exception as exc:  # 演练记录错误但不停整体
                with self.lock:
                    self.errors.append(f'{name}: {type(exc).__name__}: {exc}')
            time.sleep(0.02)

    def _worker_loop(self, name: str):
        while not self._stop.is_set():
            with self.lock:
                target = self._pending_reviews.pop(0) if self._pending_reviews else None
            if target is None:
                time.sleep(0.02)
                continue
            try:
                evidence_id = workload.complete_run(self.db_path, target)
                with self.lock:
                    self.completed_runs += 1
                    self._pending_evidence = getattr(self, '_pending_evidence', [])
                    self._pending_evidence.append(evidence_id)
            except Exception as exc:
                with self.lock:
                    self.errors.append(f'{name}: {type(exc).__name__}: {exc}')

    def _reviewer_loop(self, name: str):
        while not self._stop.is_set():
            with self.lock:
                queue = getattr(self, '_pending_evidence', [])
                target = queue.pop(0) if queue else None
            if target is None:
                time.sleep(0.02)
                continue
            try:
                workload.review_evidence(
                    self.db_path, target,
                    f'{name} 夜班复核通过 {utc_now()}', 'reviewer')
                with self.lock:
                    self.reviews += 1
            except Exception as exc:
                with self.lock:
                    self.errors.append(f'{name}: {type(exc).__name__}: {exc}')

    def start(self) -> None:
        roles = [('op-1', self._operator_loop), ('op-2', self._operator_loop),
                 ('worker', self._worker_loop), ('reviewer', self._reviewer_loop)]
        for name, target in roles[:max(2, min(4, self.scale))]:
            thread = threading.Thread(target=target, args=(name,), name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self, timeout: float = 15) -> dict:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=timeout)
        return self.stats()

    def stats(self) -> dict:
        with self.lock:
            return {
                'created_cases': self.created_cases,
                'completed_runs': self.completed_runs,
                'reviews': self.reviews,
                'errors': list(self.errors),
                'pending_reviews': len(self._pending_reviews),
                'pending_evidence': len(getattr(self, '_pending_evidence', [])),
            }


def enable_wal(db_path: str) -> None:
    """WAL 让备份快照读与并发写互不阻塞（生产 InnoDB 一致性读同理）。"""
    conn = connect(db_path)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
        conn.commit()
    finally:
        conn.close()


def source_head_seq(db_path: str) -> int:
    conn = connect(db_path, readonly=True)
    try:
        return max_seq(conn)
    finally:
        conn.close()
