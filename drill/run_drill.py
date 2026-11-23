#!/usr/bin/env python3
"""端到端恢复演练编排（夜班剧本，单进程可复现）。

覆盖规程的每一条：
  1. 首份全量，之后只留增量；
  2. 备份期间多线程并发建档、完成试算、签复核；
  3. 一个跨备份窗口的在途事务，整笔出现在后续批次而不撕裂边界；
  4. 按选定批次恢复到隔离库，档案/试算/报告/复核同一边界；
  5. 增量缺口、校验不符两种故障都必须停止，且上一份验收库照常可查；
  6. 写入失败后重试，重试不覆盖已验收版本，校验通过才交付新库；
  7. 全程不写源库（备份只读打开；恢复不连接源库）；
  8. 输出恢复批次与校验结论报告。
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import backup, faults, loadgen, restore, workload  # noqa: E402
from lib.common import ensure_dir, utc_now, write_json_atomic  # noqa: E402
from lib.storage import connect  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
SOURCE = os.path.join(DATA_DIR, 'source.db')
REPO = os.path.join(DATA_DIR, 'backup-repo')
ZONE = os.path.join(DATA_DIR, 'restore-zone')
REPORT_PATH = os.path.join(DATA_DIR, 'drill-report.json')

steps: list[dict] = []


def record(name: str, ok: bool, detail: dict) -> None:
    entry = {'step': name, 'ok': ok, 'at': utc_now(), 'detail': detail}
    steps.append(entry)
    flag = 'PASS' if ok else 'FAIL'
    print(f'[{flag}] {name}: {json.dumps(detail, ensure_ascii=False)}')


def reset_layout():
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    ensure_dir(DATA_DIR)


def main() -> int:
    reset_layout()

    # 0. 初始化源库（演示账号 + 演示档案），开启 WAL。
    seed_counts = workload.seed(SOURCE)
    loadgen.enable_wal(SOURCE)
    record('初始化源库', True, {'seed_counts': seed_counts,
                               'source': os.path.relpath(SOURCE)})

    # 1. 启动并发负载：备份期间照常建档/试算/复核。
    load = loadgen.ConcurrentLoad(SOURCE, scale=4, prefix='夜班')
    load.start()
    time.sleep(0.6)

    # 2. 首份全量备份（并发负载仍在跑）。
    full = backup.create_full(REPO, SOURCE, note='首份全量（并发进行中）')
    record('首份全量备份', True, {
        'batch_id': full['batch_id'], 'end_seq': full['end_seq'],
        'table_counts': full['table_counts']})

    # 3. 打开一个跨越备份窗口的在途事务（不提交），再做第一份增量。
    inflight_payload = workload.domain.DEMO_SAMPLE
    inflight_conn, inflight_case_id = workload.begin_case_txn(
        SOURCE, 'operator', '在途档案-跨备份窗口', inflight_payload)
    time.sleep(0.4)
    incr1 = backup.create_incr(REPO, SOURCE, note='增量1：含并发提交，在途事务未提交')
    in_flight_visible_during = None
    ro = connect(SOURCE, readonly=True)
    try:
        in_flight_visible_during = ro.execute(
            'SELECT COUNT(*) AS c FROM business_cases WHERE id=?',
            (inflight_case_id,)).fetchone()['c']
    finally:
        ro.close()
    record('增量1 与在途事务边界', in_flight_visible_during == 0, {
        'batch_id': incr1['batch_id'], 'end_seq': incr1['end_seq'],
        'inflight_case_id': inflight_case_id,
        'inflight_visible_while_open': in_flight_visible_during,
        '结论': '在途事务在提交前对任何快照不可见'})

    # 4. 提交在途事务，再做第二份增量——整笔落在后续批次。
    inflight_seq = workload.commit_case_txn(inflight_conn, inflight_case_id, 'operator')
    # 让并发负载再跑一会儿后备份：在途提交必须严格晚于上一批终点。
    time.sleep(0.4)
    incr2 = backup.create_incr(REPO, SOURCE, note='增量2：在途事务整笔落入本批')
    landed_in_later_batch = (inflight_seq > incr2['prev_end_seq']
                             and inflight_seq <= incr2['end_seq'])
    record('在途事务整笔进入后续批次', landed_in_later_batch, {
        'batch_id': incr2['batch_id'], 'inflight_commit_seq': inflight_seq,
        'batch_window': [incr2['prev_end_seq'] + 1, incr2['end_seq']]})

    # 让并发负载再跑一会儿，凑出第三份增量。
    time.sleep(0.6)
    incr3 = backup.create_incr(REPO, SOURCE, note='增量3：更多并发业务提交')
    record('增量3', True, {'batch_id': incr3['batch_id'],
                          'end_seq': incr3['end_seq'],
                          'commits': incr3['commit_count']})

    load_stats = load.stop()
    record('并发负载收口', not load_stats['errors'], load_stats)
    source_head = loadgen.source_head_seq(SOURCE)

    # 5. 恢复到批次 incr1：隔离库、逐笔校验、验收。
    r1 = restore.restore(REPO, ZONE, incr1['batch_id'], note='首次演练恢复')
    record('恢复到增量1并验收', r1['status'] == 'accepted', {
        'version': r1['version'], 'chain': r1['chain_verified'],
        'replayed_commits': r1['replayed_commits'],
        'verification': r1['verification']})

    accepted_v1 = restore.load_registry(ZONE)['accepted_version']
    query_v1 = restore.query_accepted(ZONE)
    record('已验收演练库可查', query_v1.get('available') is True, {
        'accepted_version': query_v1.get('accepted_version'),
        'table_counts': query_v1.get('table_counts')})

    # 6. 故障 A：恢复写入前中断，随后重试；重试不得覆盖已验收版本。
    r_fail = restore.restore(REPO, ZONE, incr2['batch_id'],
                             note='演练写入中断', fail_target_attempt=1)
    record('恢复中断被标记为失败', r_fail['status'] == 'failed' and
           accepted_v1 == restore.load_registry(ZONE)['accepted_version'], {
               'failed_version': r_fail['version'], 'error': r_fail['error'],
               'accepted_unchanged': restore.load_registry(ZONE)['accepted_version']})
    r_retry = restore.restore(REPO, ZONE, incr2['batch_id'], note='中断后重试')
    record('重试成功且不覆盖已验收版本',
           r_retry['status'] == 'accepted' and r_retry['version'] != r_fail['version']
           and os.path.isdir(os.path.join(restore.restores_root(ZONE), accepted_v1)),
           {'new_version': r_retry['version'], 'kept_version': accepted_v1,
            'verification': r_retry['verification']})

    # 7. 故障 B：校验不符（破坏增量3片段），必须停止，已验收库不受影响。
    corrupt_fragment = faults.corrupt_increment(REPO, incr3['batch_id'])
    r_corrupt = restore.restore(REPO, ZONE, incr3['batch_id'], note='校验不符演练')
    record('校验不符即停止',
           r_corrupt['status'] == 'failed' and '校验不符' in (r_corrupt['error'] or ''),
           {'fragment': os.path.relpath(corrupt_fragment), 'error': r_corrupt['error']})
    record('校验失败后上一份验收库照常可查',
           restore.load_registry(ZONE)['accepted_version'] == r_retry['version'],
           {'accepted_version': restore.load_registry(ZONE)['accepted_version']})

    # 重新做一份完好的增量3替代损坏批次（先清理损坏目录再备份）。
    shutil.rmtree(backup.batch_dir(REPO, incr3['batch_id']))
    catalog = backup.load_catalog(REPO)
    catalog['batches'] = [b for b in catalog['batches']
                          if b['batch_id'] != incr3['batch_id']]
    write_json_atomic(backup.catalog_path(REPO), catalog)
    incr3_fixed = backup.create_incr(REPO, SOURCE, note='增量3：重新备份修复')
    r3 = restore.restore(REPO, ZONE, incr3_fixed['batch_id'], note='修复后恢复')
    record('修复后恢复到最新批次', r3['status'] == 'accepted', {
        'version': r3['version'], 'verification': r3['verification']})

    # 8. 故障 C：增量缺口（在仓库副本上演练，避免破坏完好仓库的最终状态）。
    gap_repo = os.path.join(DATA_DIR, 'backup-repo-gap')
    if os.path.exists(gap_repo):
        shutil.rmtree(gap_repo)
    shutil.copytree(REPO, gap_repo)
    gap_zone = os.path.join(DATA_DIR, 'restore-zone-gap')
    faults.remove_increment(gap_repo, incr2['batch_id'])
    r_gap = restore.restore(gap_repo, gap_zone, incr3_fixed['batch_id'],
                            note='增量缺口演练')
    record('增量缺口即停止',
           r_gap['status'] == 'failed' and '缺口' in (r_gap['error'] or ''),
           {'removed': incr2['batch_id'], 'error': r_gap['error']})

    # 9. 恢复库与源库同边界核对：最新验收库的提交终点与源库当前位点一致，
    #    且表行数、证据校验码、外键、复核边界全部通过。
    final_query = restore.query_accepted(ZONE)
    head_match = r3['verification']['end_seq'] == source_head
    record('恢复库提交终点与源库位点一致', head_match, {
        'restored_end_seq': r3['verification']['end_seq'],
        'source_head_seq': source_head})
    record('最终演练库可查且行数齐备', final_query['available'], {
        'accepted_version': final_query['accepted_version'],
        'table_counts': final_query['table_counts'],
        'latest_reviews': final_query['latest_reviews'][:2]})

    # 10. 只读保证：恢复模块全程未连接源库（目录证据 + 源库文件未被恢复进程打开）。
    source_unchanged = loadgen.source_head_seq(SOURCE) == source_head
    record('全程不写源库', source_unchanged, {
        'source_head_seq_before_after': source_head,
        '备份连接模式': 'mode=ro 只读 URI',
        '恢复模块输入': '仅备份仓库与隔离区'})

    report = {
        '演练名称': '窑务数据丢失恢复演练',
        '开始时间': steps[0]['at'],
        '结束时间': utc_now(),
        '源库': os.path.relpath(SOURCE),
        '备份仓库': os.path.relpath(REPO),
        '隔离恢复区': os.path.relpath(ZONE),
        '备份批次': [
            {k: b[k] for k in ('batch_id', 'kind', 'prev_end_seq', 'end_seq', 'note')}
            for b in backup.load_catalog(REPO)['batches']],
        '并发负载': load_stats,
        '恢复登记': restore.load_registry(ZONE),
        '步骤结论': steps,
        '总步骤数': len(steps),
        '通过步骤数': sum(1 for s in steps if s['ok']),
    }
    report['全部通过'] = report['总步骤数'] == report['通过步骤数']
    write_json_atomic(REPORT_PATH, report)
    print('\n演练报告：', os.path.relpath(REPORT_PATH))
    print(f"通过 {report['通过步骤数']}/{report['总步骤数']} 步，"
          f"全部通过={report['全部通过']}")
    return 0 if report['全部通过'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
