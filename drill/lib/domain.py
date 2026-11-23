"""试算领域逻辑（移植自 backend/src/services/domain.py，保持同一套算法与报告）。

只做纯计算，不依赖 Web 框架，供并发建档/试算工作负载调用。
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class Node:
    weight: int | float
    capacity: int
    other_used: int


@dataclass(frozen=True)
class Batch:
    identity: str
    current_owner: str
    size: int


@dataclass(frozen=True)
class Snapshot:
    identity: str
    budget: int
    nodes: dict[str, Node]
    batches: list[Batch]
    initial_used: dict[str, int]


def _identifier(value, label):
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f'{label}须为非空文本，且首尾不能有空白')
    value.encode('utf-8')
    return value


def _byte_count(value, minimum, label):
    if type(value) is not int or value < minimum:
        raise ValueError(f'{label}须为不小于 {minimum} 的整数字节数')
    return value


def _rows(payload, key, label):
    rows = payload.get(key)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f'{label}须为对象列表')
    return rows


def read_snapshot(payload) -> Snapshot:
    if not isinstance(payload, dict):
        raise ValueError('分片盘点须为对象')
    identity = _identifier(payload.get('snapshot_id'), '盘点编号')
    budget = _byte_count(payload.get('round_budget_bytes'), 0, '轮次预算')

    nodes = {}
    for index, row in enumerate(_rows(payload, 'nodes', '窑口节点'), start=1):
        node_id = _identifier(row.get('node_id'), f'第 {index} 行节点编号')
        if node_id in nodes:
            raise ValueError(f'节点编号重复：{node_id}')
        weight = row.get('weight')
        if (type(weight) not in (int, float) or weight < 0
                or (type(weight) is float and not isfinite(weight))):
            raise ValueError(f'节点 {node_id} 的目标权重须为有限非负数')
        capacity = _byte_count(row.get('capacity_bytes'), 0, f'节点 {node_id} 的物理容量')
        other_used = _byte_count(row.get('other_used_bytes'), 0, f'节点 {node_id} 的其他占用')
        nodes[node_id] = Node(weight, capacity, other_used)
    if not any(node.weight > 0 for node in nodes.values()):
        raise ValueError('至少需要一个目标权重大于零的节点')

    initial_used = {node_id: node.other_used for node_id, node in nodes.items()}
    batches = []
    seen = set()
    for index, row in enumerate(_rows(payload, 'batches', '巡检批次'), start=1):
        batch_id = _identifier(row.get('batch_id'), f'第 {index} 行批次编号')
        if batch_id in seen:
            raise ValueError(f'批次编号重复：{batch_id}')
        seen.add(batch_id)
        owner = _identifier(row.get('current_owner'), f'批次 {batch_id} 的现有归属')
        if owner not in nodes:
            raise ValueError(f'批次 {batch_id} 的现有归属 {owner} 未在节点盘点中')
        size = _byte_count(row.get('size_bytes'), 1, f'批次 {batch_id} 的占用')
        batches.append(Batch(batch_id, owner, size))
        initial_used[owner] += size

    for node_id, used in initial_used.items():
        capacity = nodes[node_id].capacity
        if used > capacity:
            raise ValueError(f'节点 {node_id} 的盘点占用 {used} 字节超过物理容量 {capacity} 字节')
    batches.sort(key=lambda batch: batch.identity)
    return Snapshot(identity, budget, dict(sorted(nodes.items())), batches, initial_used)


def validate(payload) -> None:
    read_snapshot(payload)


def _score(batch_id, node_id, log_weight):
    key = json.dumps([batch_id, node_id], ensure_ascii=False,
                     separators=(',', ':')).encode('utf-8')
    prefix = int.from_bytes(hashlib.sha256(key).digest()[:7], 'big') >> 4
    uniform = (prefix + 0.5) / (1 << 52)
    return math.log(-math.log(uniform)) - log_weight


def _schedule(snapshot, changed, proposed):
    used = snapshot.initial_used.copy()
    remaining = snapshot.budget
    manifest = []
    pending = sorted(
        changed,
        key=lambda batch: (snapshot.nodes[batch.current_owner].weight > 0, batch.identity))
    while pending:
        moves_before = len(manifest)
        waiting_for_space = []
        for batch in pending:
            source = batch.current_owner
            target = proposed[batch.identity]
            if batch.size > remaining:
                continue
            available = snapshot.nodes[target].capacity - used[target]
            if batch.size > available:
                waiting_for_space.append(batch)
                continue
            used[target] += batch.size
            used[source] -= batch.size
            remaining -= batch.size
            manifest.append({
                '序号': len(manifest) + 1,
                '盘点编号': snapshot.identity,
                '批次编号': batch.identity,
                '源节点': source,
                '目标节点': target,
                '搬迁字节': batch.size,
                '复制前目标可用字节': available,
                '完成后目标可用字节': available - batch.size,
                '完成后预算余量字节': remaining,
            })
        if len(manifest) == moves_before:
            break
        pending = waiting_for_space
    return manifest, used, remaining


def _deferral(size, free, remaining, initial_budget):
    reasons = []
    if size > free:
        reasons.append('目标容量不足')
    if size > initial_budget:
        reasons.append('单批超过整轮预算')
    elif size > remaining:
        reasons.append('本轮剩余预算不足')
    return '、'.join(reasons)


def solve(payload) -> dict:
    snapshot = read_snapshot(payload)
    log_weights = {
        node_id: math.log(node.weight)
        for node_id, node in snapshot.nodes.items() if node.weight > 0
    }
    proposed = {
        batch.identity: min(
            log_weights,
            key=lambda node_id: (_score(batch.identity, node_id,
                                        log_weights[node_id]), node_id))
        for batch in snapshot.batches
    }
    changed = [batch for batch in snapshot.batches
               if proposed[batch.identity] != batch.current_owner]
    manifest, used, remaining = _schedule(snapshot, changed, proposed)
    planned = {row['批次编号'] for row in manifest}

    desired_used = {node_id: node.other_used
                    for node_id, node in snapshot.nodes.items()}
    comparisons = []
    for batch in snapshot.batches:
        target = proposed[batch.identity]
        desired_used[target] += batch.size
        if target == batch.current_owner:
            status, reason = '保留', '归属一致'
        elif batch.identity in planned:
            status, reason = '本轮搬迁', '已列入清单'
        else:
            status = '待排'
            reason = _deferral(batch.size,
                               snapshot.nodes[target].capacity - used[target],
                               remaining, snapshot.budget)
        comparisons.append({
            '批次编号': batch.identity,
            '批次字节': batch.size,
            '现有归属': batch.current_owner,
            '计算归属': target,
            '本轮结束归属': target if batch.identity in planned else batch.current_owner,
            '判定': status,
            '原因': reason,
        })

    node_rows = [{
        '节点编号': node_id,
        '目标权重': node.weight,
        '物理容量字节': node.capacity,
        '其他占用字节': node.other_used,
        '盘点占用字节': snapshot.initial_used[node_id],
        '完全按计算归属时占用字节': desired_used[node_id],
        '本轮结束占用字节': used[node_id],
        '本轮结束可用字节': node.capacity - used[node_id],
    } for node_id, node in snapshot.nodes.items()]

    deferred_count = len(changed) - len(manifest)
    moved_bytes = snapshot.budget - remaining
    return {
        'summary': (f'盘点 {snapshot.identity} 有 {len(changed)} 批归属变化，'
                    f'本轮可搬 {len(manifest)} 批共 {moved_bytes} 字节，'
                    f'待排 {deferred_count} 批。'),
        'metrics': {
            '批次总数': len(snapshot.batches),
            '归属变化批次': len(changed),
            '归属变化字节': sum(b.size for b in changed),
            '计划搬迁批次': len(manifest),
            '计划搬迁字节': moved_bytes,
            '待排批次': deferred_count,
            '轮次预算字节': snapshot.budget,
            '剩余预算字节': remaining,
        },
        'sections': [
            {'title': '本轮搬迁清单', 'rows': manifest},
            {'title': '批次归属比较', 'rows': comparisons},
            {'title': '节点物理占用', 'rows': node_rows},
        ],
    }


DEMO_SAMPLE = {
    'snapshot_id': 'TY-20261121-0800',
    'round_budget_bytes': 150_000_000,
    'nodes': [
        {'node_id': 'EAST', 'weight': 1, 'capacity_bytes': 240_000_000,
         'other_used_bytes': 60_000_000},
        {'node_id': 'NORTH', 'weight': 3, 'capacity_bytes': 160_000_000,
         'other_used_bytes': 65_000_000},
        {'node_id': 'WEST', 'weight': 2, 'capacity_bytes': 176_000_000,
         'other_used_bytes': 45_000_000},
        {'node_id': 'OLD', 'weight': 0, 'capacity_bytes': 170_000_000,
         'other_used_bytes': 20_000_000},
    ],
    'batches': [
        {'batch_id': 'TY-1121-01', 'current_owner': 'EAST', 'size_bytes': 32_000_000},
        {'batch_id': 'TY-1121-02', 'current_owner': 'OLD', 'size_bytes': 70_000_000},
        {'batch_id': 'TY-1121-03', 'current_owner': 'OLD', 'size_bytes': 50_000_000},
        {'batch_id': 'TY-1121-04', 'current_owner': 'EAST', 'size_bytes': 55_000_000},
        {'batch_id': 'TY-1121-05', 'current_owner': 'EAST', 'size_bytes': 30_000_000},
    ],
}
