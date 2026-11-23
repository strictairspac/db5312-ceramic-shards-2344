# 窑务数据丢失恢复演练

夜班演练工具，验证“全量 + 增量备份 → 隔离库恢复 → 校验交付”的完整规程。
**仅依赖 Python 3.11 标准库**（本机环境无 Docker/MySQL/pip），用 SQLite 模拟
生产 MySQL；存储引擎差异的适配点见下文“生产映射”。演练代码不改动 `backend/`
任何业务代码，领域试算算法是 `backend/src/services/domain.py` 的等价移植。

## 规程对应关系

| 规程要求 | 落地实现 |
| --- | --- |
| 首份备份之后只留增量 | `backup.create_full` 只允许首份；之后 `create_incr` 按位点追加，拒绝第二份全量、拒绝空增量 |
| 备份期间照常并发建档、完成试算、签复核 | `loadgen.py` 4 线程并发（2 经办 + 1 worker + 1 复核），WAL 下与在线备份互不阻塞 |
| 按选定备份批次恢复到隔离库 | `restore.restore(repo, zone, batch_id)`，每次恢复生成独立版本目录 `restores/restore-<批次>-attemptNN/` |
| 档案、试算、报告、复核对应同一提交边界 | 每个业务动作是一个事务，事务内同时写业务表并追加 `commit_log`；恢复逐事务重放。校验项含 complete↔证据配对、复核三字段同生同空、外键、证据 SHA-256 |
| 在途事务整笔留给后续批次 | 显式长事务跨备份窗口：未提交时任何快照不可见，提交后整笔进入下一个增量批次（演练第 3、4 步） |
| 增量有缺口或校验不符就停止 | 位点链 `prev_end_seq` 断链、文件/逐行 SHA-256 不符均抛 `RestoreChainError`，不写隔离库业务数据 |
| 上一份验收通过的演练库照常可查 | `registry.json` 记录 `accepted_version`；失败不改验收指针，`restore query` 只读打开旧库 |
| 中断/写入失败可重试，重试不覆盖已验收版本 | 每次尝试独立版本目录；只有全部校验通过才切换验收指针；旧版本目录存在性被二次检查 |
| 校验通过才交出新演练库 | `verify_database`：位点连续性、外键、证据 digest、配对边界、源库同快照行数五项全过才 `accepted` |
| 全程不写源库 | 备份连接固定 `file:...?mode=ro` 只读 URI；恢复模块根本不接收源库路径，只依赖份仓库与隔离区 |
| 结果写清恢复批次和校验结论 | `data/drill-report.json`（17 步逐条结论）+ 每版本 `result.json` + `registry.json` |

## 运行

```sh
python3 -I run_drill.py      # 端到端演练（重置 data/ 后执行全部场景）
python3 -I verify_drill.py   # 独立复核：逐行比对源库与演练库、重算证据 digest
```

`-I` 隔离模式保证不会从数据目录加载任何旁路模块。

CLI 手动操作：

```sh
python3 -I drill_cli.py init-source
python3 -I drill_cli.py backup --note "夜班批次"
python3 -I drill_cli.py list-batches
python3 -I drill_cli.py verify-chain incr-0003
python3 -I drill_cli.py restore incr-0003 --fail-target 1   # 注入首次中断
python3 -I drill_cli.py restore incr-0003                   # 重试
python3 -I drill_cli.py query
python3 -I drill_cli.py list-restores
python3 -I drill_cli.py inject-gap incr-0003                # 故障演练（建议先复制仓库）
python3 -I drill_cli.py inject-corrupt incr-0004
```

## 目录产物（`data/`，运行后生成）

```
data/
  source.db                       # 模拟源库（五张业务表 + commit_log）
  backup-repo/
    catalog.json                  # 批次清单与位点链（原子写）
    full-0001/                    # 全量：snapshot.sqlite + rows/*.jsonl + manifest
    incr-0002/ …                  # 增量：commits/*.jsonl（逐行 SHA-256）+ manifest
  restore-zone/
    registry.json                 # 验收指针与全部尝试登记
    restores/restore-…-attemptNN/ # 每个尝试一个隔离库版本 + result.json
  drill-report.json               # 演练总报告（恢复批次 + 校验结论）
```

## 校验机制

1. **备份链**：全量快照内部位点与清单一致；每批 `prev_end_seq` 必须接上批 `end_seq`；
   每个文件按 manifest 中的 SHA-256 与字节数核对；增量逐行再算一次哈希。
2. **重放**：增量按 `commit_log` 顺序，一笔提交一个事务重放业务表并原样登记日志，
   保持提交边界；INSERT/UPDATE 按主键幂等处理。
3. **恢复后**：`PRAGMA foreign_key_check`、位点 1..N 连续、证据 digest 重算
   （规则同生产：紧凑 UTF-8、键排序 JSON 的 SHA-256）、complete 必有证据、
   复核三字段边界、表行数与备份时源库同快照计数一致。

## 生产 MySQL 映射

| 本演练（SQLite/文件） | 生产 MySQL |
| --- | --- |
| `mode=ro` 只读连接 | 专用备份账号仅授 SELECT/REPLICATION CLIENT |
| 在线 `backup()` 一致性快照 | `mysqldump --single-transaction --master-data=2`（GTID 环境记录 GTID 集） |
| `commit_log.seq` 位点 | binlog 位点（file + position）或 GTID |
| 增量 jsonl 片段 | `mysqlbinlog --stop-never` 按位点切批，每批附起止位点与 SHA-256 |
| 逐事务重放 | `mysqlbinlog | mysql` 到隔离实例，或 SQL 应用器按事务边界提交 |
| 隔离版本目录 | 隔离实例上独立 database/schema，验收前不切换连接串 |
| `registry.json` 验收指针 | 恢复编排元数据表/配置，只在校验全过后更新 |

业务表结构与 `backend/src/models/tables.py` 同名同构；`commit_log` 是演练为
显式表达“提交边界”引入的等价物，生产由 binlog 承担，不需要业务侧改造。
