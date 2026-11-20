# 巡检工单库交接演示

FastAPI + 双 PostgreSQL 15（逻辑复制）实现的工单库单向交接：旧库（发布端）
暂停接单、等在途写入结束、新库（订阅端）收齐已提交工单并对齐序列后切换接单。

## 目录结构

```
.
├── docker-compose.yml            # old-db / new-db / app 三服务（交付目标）
├── app/                          # FastAPI 应用（含 Dockerfile、requirements）
│   └── app/
│       ├── main.py               # 路由与演示网页（工单、交接、实时进度、日志）
│       ├── cutover.py            # 交接状态机：后台线程 + 阶段化进度 + 重启恢复
│       ├── state.py              # 接单库/交接运行持久化（SQLite，事务保证原子生效）
│       ├── db.py                 # PG 访问：工单写入、序列快照、复制状态查询
│       └── config.py             # 环境变量配置（轮询间隔默认 0.5s）
├── db/old/init/01_schema.sql     # 旧库：表 + CACHE 5 序列 + 发布
├── db/new/init/                  # 新库：同名表/序列 + 订阅创建脚本
├── scripts/
│   ├── local_pg.sh               # 无 Docker 时用本地 PG 二进制起两库并配置复制
│   └── local_app.sh              # 无 Docker 时本地启动应用
└── tests/e2e_test.py             # 端到端试跑（46 项断言，含生效前后重启）
```

## 方式一：Docker Compose（交付目标）

```bash
docker compose up --build
# 应用（交接页）http://localhost:8000 ；旧库 5433、新库 5434
```

- `old-db` 以 `wal_level=logical` 启动并创建发布 `inspection_pub`；
- `new-db` 启动后带重试创建订阅 `inspection_sub`（`copy_data=true` 初始同步）；
- 应用状态保存在 `app-data` 卷，容器重启后沿用已生效的接单库与交接结果；
- 三个容器均带 healthcheck，应用 `/api/status` 不健康时 Compose 可感知。

## 方式二：本机无 Docker 试跑（本仓库实际验证方式）

当前环境没有 Docker，用解包到用户目录的 PostgreSQL 15 二进制起两库：

```bash
./scripts/local_pg.sh start      # 初始化并启动两库 + 逻辑复制（55433 旧 / 55434 新）
./scripts/local_app.sh           # 启动 FastAPI（http://127.0.0.1:8000，超时阈值 8s）
python -I tests/e2e_test.py      # 端到端试跑（脚本自行启停应用，含 SIGKILL 崩溃）
./scripts/local_pg.sh status     # 查看两库行数与订阅状态
./scripts/local_pg.sh stop       # 停止；clean 可清理全部本地数据
```

## 交接语义与关键设计

1. **统一入口**：网页 `POST /api/work-orders` 按当前接单库路由，响应返回工单
   编号与 `accepted_by`（old/new）。
2. **异步交接 + 串行化**：`POST /api/cutover`（需 `confirm=true`）立即返回 `202`
   与 `run_id`，交接在后台线程执行；进行中再次发起返回 `409`，NEW 下重复发起
   幂等返回当前接单库。
3. **旧库暂停接单**：进入 SWITCHING 后网页提交返回 503。
4. **半秒检查与实时进度**：后台每 `poll_interval_seconds`（默认 0.5s）检查一次
   在途写入与复制进度，读数（在途写入数、`current_lsn`/`replay_lsn`、复制状态、
   两库行数）只写入内存，经 `GET /api/progress?run_id=...` 供网页实时展示。
5. **持久日志只随阶段/结果变化**：SQLite 事件表只在阶段变化
   （`SWITCHING → WAIT_REPLAY → ALIGN_SEQUENCE → COMMIT`）与最终结果（生效/回滚）
   时写入；反复检查同一阶段的半秒读数不再落盘，避免日志刷屏。
6. **读数与同一次交接绑定**：每次交接分配唯一 `run_id`，网页等待卡片只展示该
   `run_id` 的阶段与读数；查询别的 run_id 返回 `stale`，不会把不同次交接的等待
   状态串到一起。出结果后页面结束等待显示。
7. **等在途写入结束**：进程内在途计数 + 旧库 `pg_stat_activity` 中
   `xact_start/backend_xid` 非空的客户端事务双重确认（覆盖 idle in transaction）。
8. **收齐才接单**：轮询发布端 `pg_current_wal_lsn()` 与
   `pg_stat_replication.replay_lsn` 一致且 `state=streaming`，再核对两库行数与
   最大编号一致。
9. **编号接续（含预留号）**：序列 `CACHE 5`。`pg_sequence_last_value()` 返回的即
   “已预留到的最高号”，已包含缓存未用号与事务回滚空号。新库
   `setval(seq, 预留最高号, true)`，首个新编号 = 预留最高号 + 1。
10. **失败/超时回滚**：任一步异常或超过 `CUTOVER_TIMEOUT_SECONDS` 未追平，状态
    回到 OLD，旧库恢复接单，事件日志与运行记录写明原因。
11. **原子生效（接单库 + 最终事件同一事务）**：`commit_cutover` 在单个 SQLite
    事务里同时写入 NEW 接单状态、最终事件与 COMPLETED 运行记录；开始交接时
    `begin_cutover` 同样在单事务写入 SWITCHING 与 INTERRUPTED 运行记录。
12. **重启恢复**：
    - **生效前退出**：运行记录停在 INTERRUPTED，重启时自动恢复旧库接单，并向
      事件日志补记本次交接中断（含中断时所处阶段）；
    - **生效后退出**：NEW 与 COMPLETED 已同事务提交，重启仍由新库接单，
      `GET /api/runs/{run_id}` 可查到原交接结果。
13. **订阅暂停/恢复**：网页按钮执行 `ALTER SUBSCRIPTION DISABLE/ENABLE`。暂停
    期间旧库继续写入制造复制积压，发起交接即可在实时进度卡片观察长等待，
    超时后自动回滚。

## 主要 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/status` | 接单状态、在途写入数、订阅状态、活动进度与重启恢复信息 |
| `POST /api/cutover` | 发起交接（`confirm=true`），返回 202 + `run_id`；409 进行中冲突 |
| `GET /api/progress?run_id=` | 该次交接的实时阶段与最近检查读数（内存，不落盘） |
| `GET /api/runs` / `GET /api/runs/{id}` | 交接运行记录与最终结果（持久化，重启可查） |
| `GET /api/events` | 持久事件日志（仅阶段变化与结果） |
| `POST /api/subscription/pause` / `resume` | 订阅暂停/恢复 |

## 已验证结果（tests/e2e_test.py，46/46 通过）

- 旧库受理工单并自动编号；逻辑复制实时同步到新库；
- 暂停订阅 → 交接长等待（每 0.5s 一条内存检查，进度含在途数与 LSN）→ 8s 超时
  → 回滚 OLD，旧库恢复接单；持久事件仅 3 条（开始/阶段变化/回滚），无轮询刷屏；
- `run_id` 隔离：不同交接的读数互不串用，结果出来后等待结束；
- 恢复订阅追平后，在途慢事务结束才完成交接；
- 缓存未用号/回滚空号计入预留最高号，首个新编号严格接续；
- **生效前 SIGKILL 重启**：恢复旧库接单、补记中断事件、运行记录可查；
- **生效后重启**：仍由新库接单，原 COMPLETED 结果可查，新库编号继续递增；
- NEW 下重复交接幂等；SWITCHING 期间提交 503、重复发起 409。

## 已知边界/剩余事项

- 状态机为单应用进程设计（在途计数在进程内、半秒进度在内存）；多副本部署需把
  在途协调放到数据库（如 `pg_advisory_lock`/状态表），本演示未包含。
- 切换到 NEW 后未实现回切；重置演示请 `docker compose down -v`（本地为
  `./scripts/local_pg.sh clean`）。
- 生产化还需补充：认证授权、TLS、连接池、监控指标与告警、旧库最终只读/下线流程。
