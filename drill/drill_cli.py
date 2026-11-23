#!/usr/bin/env python3
"""窑务数据丢失恢复演练 CLI（仅依赖 Python 3.11 标准库）。

子命令：
  init-source       初始化源库并播种演示账号/档案
  load-start/stop   备份期间的并发建档/试算/复核负载
  backup            无全量则全量，其后只追加增量
  list-batches      查看备份批次与位点链
  restore           按选定批次恢复到隔离库（失败可重试，不覆盖已验收版本）
  query             只读查询当前已验收演练库
  verify-chain      只做备份链校验
  list-restores     查看隔离区恢复登记
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import backup, faults, loadgen, restore, workload  # noqa: E402


def _print(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def cmd_init_source(args):
    counts = workload.seed(args.source)
    loadgen.enable_wal(args.source)
    _print({'source': os.path.abspath(args.source), 'seeded': counts})


def cmd_backup(args):
    manifest = backup.create_auto(args.repo, args.source, note=args.note)
    _print({'batch_id': manifest['batch_id'], 'kind': manifest['kind'],
            'prev_end_seq': manifest['prev_end_seq'],
            'end_seq': manifest['end_seq'],
            'note': manifest['note']})


def cmd_list_batches(args):
    catalog = backup.load_catalog(args.repo)
    _print(catalog)


def cmd_verify_chain(args):
    manifests = restore.verify_chain(args.repo, args.batch)
    _print([{'batch_id': m['batch_id'], 'kind': m['kind'],
             'prev_end_seq': m['prev_end_seq'], 'end_seq': m['end_seq']}
            for m in manifests])


def cmd_restore(args):
    result = restore.restore(args.repo, args.zone, args.batch,
                             note=args.note, fail_on_attempt=args.fail_on,
                             fail_target_attempt=args.fail_target)
    _print(result)
    if result['status'] != 'accepted':
        sys.exit(2)


def cmd_query(args):
    _print(restore.query_accepted(args.zone))


def cmd_list_restores(args):
    _print(restore.load_registry(args.zone))


def cmd_load_start(args):
    load = loadgen.ConcurrentLoad(args.source, scale=args.workers, prefix=args.prefix)
    load.start()
    _print({'status': 'started', 'workers': args.workers,
            'hint': '用 load-stats 查看进度，load-stop 收口'})


def cmd_load_stats(args):
    # 负载由场景脚本在进程内持有；CLI 独立进程下仅报告源库位点。
    _print({'head_seq': loadgen.source_head_seq(args.source)})


def cmd_inject_gap(args):
    removed = faults.remove_increment(args.repo, args.batch)
    _print({'injected': 'increment_gap', 'removed_batch': removed})


def cmd_inject_corrupt(args):
    path = faults.corrupt_increment(args.repo, args.batch)
    _print({'injected': 'checksum_mismatch', 'fragment': path})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='窑务数据丢失恢复演练工具')
    parser.add_argument('--source', default=os.environ.get('DRILL_SOURCE', 'data/source.db'),
                        help='源库路径（默认 data/source.db）')
    parser.add_argument('--repo', default=os.environ.get('DRILL_REPO', 'data/backup-repo'),
                        help='备份仓库目录（默认 data/backup-repo）')
    parser.add_argument('--zone', default=os.environ.get('DRILL_ZONE', 'data/restore-zone'),
                        help='隔离恢复区（默认 data/restore-zone）')
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('init-source', help='初始化并播种源库').set_defaults(func=cmd_init_source)

    p = sub.add_parser('backup', help='全量或增量备份')
    p.add_argument('--note', default='')
    p.set_defaults(func=cmd_backup)

    sub.add_parser('list-batches', help='列出备份批次').set_defaults(func=cmd_list_batches)

    p = sub.add_parser('verify-chain', help='校验到指定批次的备份链')
    p.add_argument('batch')
    p.set_defaults(func=cmd_verify_chain)

    p = sub.add_parser('restore', help='按批次恢复到隔离库')
    p.add_argument('batch')
    p.add_argument('--note', default='')
    p.add_argument('--fail-on', type=int, default=None,
                   help='在全局第 N 次尝试时注入写入前中断（演练重试）')
    p.add_argument('--fail-target', type=int, default=None,
                   help='在本批次第 N 次尝试时注入写入前中断')
    p.set_defaults(func=cmd_restore)

    sub.add_parser('query', help='查询已验收演练库').set_defaults(func=cmd_query)
    sub.add_parser('list-restores', help='列出恢复尝试与验收版本') \
        .set_defaults(func=cmd_list_restores)

    p = sub.add_parser('load-start', help='启动并发负载（后台线程）')
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--prefix', default='夜班')
    p.set_defaults(func=cmd_load_start)

    sub.add_parser('load-stats', help='查看源库提交位点').set_defaults(func=cmd_load_stats)

    p = sub.add_parser('inject-gap', help='删除增量批次制造缺口')
    p.add_argument('batch')
    p.set_defaults(func=cmd_inject_gap)

    p = sub.add_parser('inject-corrupt', help='破坏增量文件制造校验不符')
    p.add_argument('batch')
    p.set_defaults(func=cmd_inject_corrupt)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except Exception as exc:
        _print({'error': f'{type(exc).__name__}: {exc}'})
        sys.exit(1)


if __name__ == '__main__':
    main()
