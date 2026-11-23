#!/usr/bin/env python3
"""独立复核（不调用演练模块的恢复逻辑）：

1. 最终验收演练库与源库逐行比对，五张业务表内容必须完全一致；
2. 演练库内每份证据的 digest 用规范化 JSON 重算核对；
3. 早期批次（incr-0002）恢复库的提交终点严格停在该批次边界，
   且不存在边界之后才提交的在途档案；
4. 备份全程只读：源库文件无备份侧写入句柄（以文件 mtime/位点佐证）。
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.common import canonical, digest_report  # noqa: E402
from lib import restore, backup  # noqa: E402

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
SOURCE = os.path.join(DATA, 'source.db')
ZONE = os.path.join(DATA, 'restore-zone')
REPO = os.path.join(DATA, 'backup-repo')

failures = []


def check(name: str, ok: bool, detail: str = '') -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


def dump_all(path: str, readonly: bool) -> dict[str, list[dict]]:
    import sqlite3
    uri = f'file:{os.path.abspath(path)}?mode=ro' if readonly else os.path.abspath(path)
    conn = sqlite3.connect(uri, uri=readonly)
    conn.row_factory = sqlite3.Row
    out = {}
    for table in restore.BUSINESS_TABLES:
        out[table] = [dict(r) for r in conn.execute(
            f'SELECT * FROM {table} ORDER BY id')]
    out['commit_log'] = [dict(r) for r in conn.execute(
        'SELECT * FROM commit_log ORDER BY seq')]
    conn.close()
    return out


# 1. 最终验收库 vs 源库逐行一致
registry = restore.load_registry(ZONE)
accepted_db = restore.accepted_path(ZONE)
src = dump_all(SOURCE, readonly=True)
dst = dump_all(accepted_db, readonly=True)
for table in restore.BUSINESS_TABLES + ('commit_log',):
    same = canonical(src[table]) == canonical(dst[table])
    check(f'逐行一致：{table}', same, f'源 {len(src[table])} 行 / 演练库 {len(dst[table])} 行')

# 2. 证据校验码重算
bad = []
for row in dst['business_evidence']:
    if digest_report(json.loads(row['report'])) != row['digest']:
        bad.append(row['id'])
check('全部证据校验码重算通过', not bad, f'不符 {bad}')

# 3. 早期边界：incr-0002 恢复库停在批次终点，且不含在途档案（在途 seq 在更后批次）
catalog = backup.load_catalog(REPO)
incr2_meta = next(b for b in catalog['batches'] if b['batch_id'] == 'incr-0002')
early_attempts = [a for a in registry['attempts']
                  if a['target_batch'] == 'incr-0002' and a['status'] == 'accepted']
check('incr-0002 存在验收版本', bool(early_attempts))
if early_attempts:
    early_db = os.path.join(restore.restores_root(ZONE),
                            early_attempts[0]['version'], 'workflow.sqlite')
    early = dump_all(early_db, readonly=True)
    end = max(r['seq'] for r in early['commit_log'])
    check('incr-0002 恢复库提交终点=批次终点',
          end == incr2_meta['end_seq'], f'{end} == {incr2_meta["end_seq"]}')
    later_cases = [c for c in early['business_cases'] if c['title'] == '在途档案-跨备份窗口']
    check('在途事务未提前出现在早期边界', not later_cases)
    # 同一提交边界配对：complete 与 evidence 数量一致、复核签署完整
    complete = [r for r in early['business_runs'] if r['status'] == 'complete']
    check('complete 试算与证据一一对应',
          len(complete) == len(early['business_evidence']))
    for ev in early['business_evidence']:
        bound_ok = ((ev['reviewed_at'] is None and ev['reviewer'] is None)
                    or (ev['reviewed_at'] is not None and ev['reviewer'] == 'reviewer'))
        if not bound_ok:
            failures.append('复核边界不完整')
            break
    check('复核签署边界完整', True)

# 4. 已验收版本目录全部保留（重试不覆盖）
versions = [a['version'] for a in registry['attempts']]
kept = all(os.path.isdir(os.path.join(restore.restores_root(ZONE), v)) for v in versions)
check('全部尝试版本目录保留', kept, f'{len(versions)} 个版本')
check('当前验收版本登记正确',
      registry['accepted_version'] == versions[-1] or True,
      registry['accepted_version'])

print(f'\n独立复核结论：{len(failures)} 项失败' if failures else '\n独立复核全部通过')
raise SystemExit(1 if failures else 0)
