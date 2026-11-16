# 海洋数仓 · 浮标观测包接收与站日目录服务

浮标站每日 Parquet 观测包入湖的接收 API。支持**双实例**共享同一存储、
**内容寻址去重**、**字节配额**、**崩溃自动回收**与完整的**接收进度查询/原件下载**，
并为分析员提供**站日候选目录查询与正式原包采纳（adoption）**能力。

技术栈：FastAPI + PyArrow + SQLite（WAL）+ uvicorn，全部阻塞 I/O 在线程池执行，
慢上传不阻塞其他接收与查询。

---

## 1. 核心能力与需求对应

| 需求 | 实现 |
| --- | --- |
| 按接收号声明站号、UTC 日期、长度、SHA-256，再上传原包 | `PUT /receipts/{id}/declaration` → `POST /receipts/{id}/content` |
| 包内含站号、UTC 时刻、盐度三列，逐行站号/站日符合声明 | `app/validation.py` 用 PyArrow 逐行校验，保留全部原始行（只读不改写） |
| 每次接收独立留档，可查进度、下载原件 | `receipts` 表每接收一行；`GET /receipts`、`GET /receipts/{id}`、`.../download` |
| 同号同声明重试沿用原记录，异声明拒绝 | 声明幂等；四要素任一不同返回 `409 declaration_conflict` |
| 配置原包字节额度，暂存与成品共用 | `OBS_QUOTA_BYTES`；口径 = 成品 blob + 在途暂存 |
| 相同内容跨接收只存一份、不重复占额 | 内容寻址 `blobs/<sha 前2位>/<sha>` + `ref_count`，硬链接发布 |
| 空间不足明确拒收 | `507 quota_exceeded`，落盘/发布前按实际内容核算 |
| 长度、摘要、内容均通过才发布，失败无可下载成品 | 三关依次校验；失败保持 `UPLOADING`，下载返回 `409` |
| 中断后无用文件及占额自动回收，未完成可重传 | 启动 + 周期 + 手动 `reconcile`，清理孤儿暂存/成品、回退缺件记录 |
| 恢复不误删另一实例在途/已发布原包，不留缺件成功记录 | 全局 flock + 写事务对账；在途暂存按 DB 归属保留，缺件成品回退而非掩盖 |
| 慢上传期间其他包仍可接收、查询 | 网络读取异步、阻塞重活进 I/O 线程池；接收级 flock 只锁同一接收号 |
| **站日候选目录：接收号、摘要、行数** | `GET /catalogs/stations/{station}/days/{date}/candidates`，只含本站日**仍已发布**接收 |
| **采纳正式原包，带目录版本乐观锁** | `POST .../adoptions`；旧版本提交 `409 catalog_version_conflict` 且不改当前选择 |
| **正式接收删除/共享原件缺失 → 目录失效** | 目录置 `INVALID` 保留失效快照，**绝不自动改选**，只能显式重新采纳 |
| **同号重新入湖即使摘要相同也须重新采纳** | 每接收号维护入湖代次 `seq`，重新入湖代次 +1，旧采纳不复活 |
| **双实例采纳/删除/对账交错与重启安全** | 状态全部落共享 SQLite + 全局 flock；读取时失效自愈，重启不复活失效接收 |
| 可启动的双实例 Docker Compose 与 Swagger 入口 | `docker-compose.yml`（app1/app2 + nginx），`/docs` Swagger |

---

## 2. 目录结构

```
app/
  config.py      环境变量配置（数据目录、额度、对账周期等）
  db.py          SQLite 连接、WAL、BEGIN IMMEDIATE 写事务、建表与 v1→v2 迁移
  errors.py      领域异常与 HTTP 状态码映射
  schemas.py     Pydantic 请求/响应模型
  storage.py     flock 文件锁、内容寻址 blob、暂存、配额、崩溃对账
  validation.py  PyArrow Parquet 三列与站号/站日逐行校验
  catalog.py     站日目录、候选查询、采纳乐观锁、失效判定与读取自愈
  service.py     声明/上传/发布/中止/删除/采纳编排，I/O 线程池
  main.py        FastAPI 路由、Swagger、后台对账、异常处理
tests/           pytest 单元/并发/回收/目录采纳测试 + 自研同步 ASGI 客户端
scripts/
  run_local.sh       无 Docker 时本机双实例启动
  integration_test.py 真实双进程端到端自测（含目录采纳交错与失效场景）
Dockerfile  docker-compose.yml  nginx.conf  requirements.txt
```

数据卷布局（双实例共享）：

```
$OBS_DATA_DIR/
  obs.db                 SQLite（WAL：obs.db-wal / obs.db-shm）
  staging/<receipt>.part 在途暂存，按接收号命名
  blobs/<xx>/<sha256>    内容寻址成品
  locks/rcpt-<id>.lock   接收级互斥锁
  locks/global.lock      发布/对账全局锁
```

数据库表（schema v2）：`receipts`（含入湖代次 `seq`）、`blobs`、
`receipt_generations`（按接收号的代次计数器）、`catalogs`（每站日一行的正式目录）。

---

## 3. 快速开始

### 方式 A：Docker Compose（推荐，双实例）

```bash
docker compose up -d --build
# 统一入口（nginx 轮询到 app1/app2）
# Swagger:  http://localhost:8080/docs
# OpenAPI:  http://localhost:8080/openapi.json
# 健康检查:  http://localhost:8080/health
```

调整额度：`OBS_QUOTA_BYTES=10485760 docker compose up -d`（默认 1 GiB）。

### 方式 B：本机双进程（无 Docker 环境，自测用）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./scripts/run_local.sh
# 实例1 http://127.0.0.1:8001/docs ；实例2 http://127.0.0.1:8002/docs
```

---

## 4. API 流程

### 第一步：声明

```bash
curl -X PUT http://localhost:8080/receipts/RCV-0001/declaration \
  -H 'Content-Type: application/json' \
  -d '{"receipt_id":"RCV-0001","station_id":"BUOY-001",
       "obs_date":"2026-11-15","size":1488,
       "sha256":"<64位十六进制>"}'
```

- 同号同声明重复调用 → 幂等返回同一记录；
- 同号异声明（站号/日期/长度/摘要任一不同）→ `409`；
- 声明长度超过总额度 → `507`。

### 第二步：上传原包（body 即 Parquet 原始字节）

```bash
curl -X POST http://localhost:8080/receipts/RCV-0001/content \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @BUOY-001_2026-11-15.parquet
```

依次通过 **长度 → SHA-256 → Parquet 三列与逐行站号/站日** 三关后发布为
`PUBLISHED`；任一失败返回 `422`，接收保持 `UPLOADING` 可重传，且**不产生成品**。

### 查询与下载

```bash
curl http://localhost:8080/receipts                         # 列表，可 ?state=&station_id=
curl http://localhost:8080/receipts/RCV-0001                # 单接收进度
curl -O -J http://localhost:8080/receipts/RCV-0001/download # 下载原件（未发布 409）
curl http://localhost:8080/quota                            # 额度占用
curl -X POST http://localhost:8080/admin/reconcile          # 手动触发回收对账
```

辅助接口：`POST /receipts/{id}/abort`（中止在途并释放暂存）、
`DELETE /receipts/{id}`（删除接收，唯一引用的成品一并回收）。

### 站日目录与采纳

同站日多份已发布观测包到达后，分析员通过下列接口指定唯一**正式原包**，
供下游按「站 + UTC 日期」查询。

```bash
# 1) 候选目录：只列本站日当前仍已发布的接收（接收号、摘要、行数）+ 当前目录版本
curl http://localhost:8080/catalogs/stations/BUOY-001/days/2026-11-15/candidates
# {
#   "station_id":"BUOY-001","obs_date":"2026-11-15",
#   "version":2,"catalog_state":"VACANT",
#   "candidates":[{"receipt_id":"RCV-0001","seq":1,"declared_sha256":"...","row_count":24,...}, ...]
# }

# 2) 查询正式目录（状态 VACANT/ACTIVE/INVALID、正式接收、版本、失效原因）
curl http://localhost:8080/catalogs/stations/BUOY-001/days/2026-11-15

# 3) 采纳：必须回传查询时拿到的 version（乐观并发控制）
curl -X POST http://localhost:8080/catalogs/stations/BUOY-001/days/2026-11-15/adoptions \
  -H 'Content-Type: application/json' \
  -d '{"receipt_id":"RCV-0001","expected_version":2}'
```

语义保证：

- **只能采纳本站日仍已发布的接收**：未发布、已删除、属于其他站日 →
  `409 adopt_not_published`；采纳前还会核对共享原件确实存在于存储。
- **目录版本乐观锁**：新候选发布、采纳、失效都会让 `version` +1。
  提交的 `expected_version` 与当前版本不一致 → `409 catalog_version_conflict`，
  **当前选择不被改变**；重新查询后用新版本再提交即可。同代次重复采纳幂等。
- **失效不自动改选**：正式接收被删除，或对账/查询发现其共享原件在磁盘缺失，
  目录置 `INVALID` 并保留失效接收快照与原因；其余候选不会被自动选中，
  必须由分析员显式重新采纳才恢复 `ACTIVE`。
- **同号重新入湖须重新采纳**：接收删除后用同一接收号再次入湖，
  入湖代次 `seq` 递增（1→2），即使 SHA-256 摘要完全相同，旧采纳也不会复活。
- **双实例与重启**：目录状态全部落在共享 SQLite，无进程内缓存；
  两实例的采纳/删除/对账经 `BEGIN IMMEDIATE` + 全局 flock 串行化，
  查询时还会做一次失效自愈核对，任何交错或重启后都不会把失效接收标为有效。

### Parquet 约束

- 必需三列：`station_id`（字符串）、`timestamp_utc`（timestamp/date/ISO8601 字符串）、
  `salinity`（数值）；允许携带额外列，原样保留。
- 每行 `station_id` 必须等于声明站号；每行时间戳的 **UTC 日期**必须等于声明日期。
- 空表拒绝；时区按 UTC 归一化处理。

---

## 5. 并发与崩溃安全设计

1. **SQLite 多进程共享**：WAL + `busy_timeout` + `BEGIN IMMEDIATE`，写事务跨实例串行化。
2. **接收级 flock**：同一接收号同时只允许一个上传会话，冲突返回 `409 receipt_busy`；
   不同接收号互不阻塞。进程崩溃时内核自动释放锁。
3. **全局 flock**：发布与对账互斥，保证回收不会与在途发布竞争。
4. **原子发布**：成品先写 `.tmp` 再 `rename`；内容已存在则只增 `ref_count`，不重复占额。
5. **对账（reconcile）**：启动即执行，之后每 `OBS_RECONCILE_INTERVAL` 秒一次——
   - 无 `UPLOADING` 归属的暂存（含他实例崩溃残留）→ 删除；
   - `UPLOADING` 缺暂存 → 进度清零、标记可重传；
   - `PUBLISHED` 缺成品 → 回退 `UPLOADING` 并解除引用，杜绝缺件成功记录；
     若该接收正是某站日正式原包，则同步把目录置 `INVALID`，否则仅推进目录版本；
   - 无引用 blob → 删除并注销；账本与磁盘不符 → 以磁盘为准修正。
6. **目录乐观锁与失效自愈**：目录版本随候选/采纳/失效单调递增，采纳在写事务内
   校验版本与接收发布状态；正式目录查询时再核对接收代次与磁盘原件，
   发现缺失当场置 `INVALID`，因此双实例交错与重启都不会让失效接收被标为有效。

---

## 6. 配额语义

- `GET /quota` 返回 `quota_bytes / used_bytes / blob_bytes / staging_bytes / available_bytes`。
- 相同内容（同 SHA-256）无论被多少接收引用，物理只存一份，`blob_bytes` 只计一次。
- 落盘暂存前按「成品已用 + 本次暂存」核算；发布新内容前按「成品已用 + 本包」核算，
  超额均返回 `507` 且不留半成品。同内容重发因发布去重，不会因暂存而被误拒。

---

## 7. 测试

```bash
pip install pytest httpx pyarrow
python -m pytest tests/                  # 47 个用例（含并发、配额、崩溃回收、目录采纳交错）
python -W error -m pytest tests/         # 严格模式（资源告警也视为失败）

# 真实双进程端到端：先按方式 B 启动两实例，再执行
OBS_DATA_DIR=/tmp/obs-e2e python -I scripts/integration_test.py
```

测试覆盖：完整发布/下载往返、幂等与冲突、长度/摘要/Parquet/站号/站日五类拒绝、
去重占额、配额 507、慢上传不阻塞、同接收并发互斥、孤儿暂存/缺件成品回收、
双 Database 连接共享状态；以及站日目录的候选过滤、版本乐观锁、采纳/改选/拒绝、
删除与缺件失效、同号重入湖代次、读取自愈、双实例采纳/删除/对账交错与重启不复活。

---

## 8. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OBS_DATA_DIR` | `/data` | 数据根目录（DB、暂存、成品、锁） |
| `OBS_QUOTA_BYTES` | `1073741824` | 原包字节总额度（暂存+成品共用） |
| `OBS_INSTANCE_ID` | `instance-0` | 实例标识，仅用于健康检查/排查 |
| `OBS_RECONCILE_INTERVAL` | `30` | 周期对账间隔秒 |
| `OBS_CHUNK_SIZE` | `1048576` | 流式分块大小 |
