"""故障注入：只允许作用于备份仓库副本/隔离区，绝不触碰源库。

- 增量缺口：从仓库中删掉一个中间增量批次并改写 catalog，模拟备份断档。
- 校验不符：破坏增量片段文件字节，模拟介质损坏导致 SHA-256 不符。
- 写入失败：由 restore.restore(fail_on_attempt=...) 注入中断。
"""
from __future__ import annotations

import os

from .common import read_json, write_json_atomic
from . import backup


def remove_increment(repo: str, batch_id: str) -> str:
    """制造增量缺口：删除指定增量批次目录及其 catalog 条目。"""
    catalog = backup.load_catalog(repo)
    entry = next((b for b in catalog['batches'] if b['batch_id'] == batch_id), None)
    if entry is None:
        raise ValueError(f'批次不存在：{batch_id}')
    if entry['kind'] != 'incremental':
        raise ValueError('只能移除增量批次制造缺口')
    path = backup.batch_dir(repo, batch_id)
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(path)
    catalog['batches'] = [b for b in catalog['batches'] if b['batch_id'] != batch_id]
    write_json_atomic(backup.catalog_path(repo), catalog)
    return batch_id


def corrupt_increment(repo: str, batch_id: str) -> str:
    """破坏增量片段内容但不动 manifest，使恢复侧哈希校验失败。"""
    manifest_path = os.path.join(backup.batch_dir(repo, batch_id), 'manifest.json')
    manifest = read_json(manifest_path)
    if manifest['kind'] != 'incremental':
        raise ValueError('只能破坏增量批次')
    fragment_rel = next(k for k in manifest['files'] if k.startswith('commits/'))
    fragment = os.path.join(backup.batch_dir(repo, batch_id), fragment_rel)
    with open(fragment, 'r+b') as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(min(40, max(0, size - 1)))
        handle.write(b'X')
    return fragment
