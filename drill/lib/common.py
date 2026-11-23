"""共用工具：规范化 JSON、哈希、时间戳与 LS。

证据校验码规则与生产保持一致：报告 JSON 按 UTF-8、键排序且省略多余空格
（紧凑分隔符）序列化后的 SHA-256，见 backend/src/utils/encoding.py。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone

_SLUG_RE = re.compile(r'[^A-Za-z0-9._-]+')


def canonical(value) -> bytes:
    """与生产 canonical() 等价的规范化序列化。"""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'),
                      sort_keys=True, allow_nan=False).encode('utf-8')


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def digest_report(report) -> str:
    return sha256_bytes(canonical(report))


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def slugify(text: str) -> str:
    cleaned = _SLUG_RE.sub('-', text).strip('-._')
    return cleaned or 'item'


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def write_json_atomic(path: str, payload) -> None:
    """同目录临时文件 + os.replace，保证清单要么是旧版要么是新版。"""
    tmp = f'{path}.tmp.{os.getpid()}'
    with open(tmp, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write('\n')
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def read_json(path: str):
    with open(path, 'r', encoding='utf-8') as handle:
        return json.load(handle)
