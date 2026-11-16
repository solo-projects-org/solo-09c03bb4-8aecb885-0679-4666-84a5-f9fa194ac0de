"""SQLite 访问层。

双实例（多进程）共享同一个数据库文件：
- WAL 模式 + NORMAL 同步，保证多进程一写多读不互斥阻塞；
- 所有写事务使用 BEGIN IMMEDIATE，配合 busy_timeout 将写串行化，
  避免 SQLITE_BUSY 直接打到请求上；
- 每个进程一个连接（uvicorn worker 内的多线程通过 RLock 串行使用）。

Schema v2 在 v1 基础上增加「站日目录 / 采纳」能力：
- receipts.seq：同一接收号的入湖代次，同号删除后重新入湖代次 +1，
  即使摘要完全相同也视为新接收，旧目录选择不会自动复活；
- receipt_generations：按接收号维护的单调代次计数器；
- catalogs：每个站日一行的正式目录，携带单调版本号 version 与
  失效状态 INVALID；采纳必须携带查询时的版本号做乐观并发控制。
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    TEXT PRIMARY KEY,
    station_id    TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    declared_size INTEGER NOT NULL CHECK (declared_size >= 0),
    declared_sha256 TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('UPLOADING', 'PUBLISHED')),
    bytes_received INTEGER NOT NULL DEFAULT 0,
    actual_sha256  TEXT,
    blob_sha256    TEXT,
    row_count      INTEGER,
    last_error     TEXT,
    seq            INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_receipts_state      ON receipts(state);
CREATE INDEX IF NOT EXISTS idx_receipts_station_day ON receipts(station_id, obs_date);

-- 内容寻址成品：sha256 唯一；ref_count 是引用它的已发布接收数
CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL CHECK (size >= 0),
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 按接收号维护的入湖代次：同号重新入湖（删除后再次声明）代次单调递增
CREATE TABLE IF NOT EXISTS receipt_generations (
    receipt_id TEXT PRIMARY KEY,
    next_seq   INTEGER NOT NULL CHECK (next_seq >= 1)
);

-- 站日正式目录：每个 (station_id, obs_date) 至多一行
-- state: VACANT（尚未采纳）/ ACTIVE（有效，已指定正式原包）/ INVALID（失效，不可自动改选）
CREATE TABLE IF NOT EXISTS catalogs (
    station_id        TEXT NOT NULL,
    obs_date          TEXT NOT NULL,
    version           INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    state             TEXT NOT NULL DEFAULT 'VACANT'
                          CHECK (state IN ('VACANT', 'ACTIVE', 'INVALID')),
    adopted_receipt_id TEXT,
    adopted_seq       INTEGER,
    adopted_sha256    TEXT,
    invalid_reason    TEXT,
    updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (station_id, obs_date)
);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


class Database:
    """进程级单例式连接封装。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"file:{self.db_path}?uri=true",
            uri=True,
            check_same_thread=False,
            isolation_level=None,  # 事务完全由代码显式管理
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        # 双实例可能在同一瞬间首次打开同一个全新数据库：journal_mode=WAL
        # 与 table_info 初始化需要排他锁，且该 PRAGMA 不受 busy_timeout 保护，
        # 因此对 "database is locked" 做有限退避重试，避免启动期竞态直接退出。
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(60):
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA foreign_keys=ON")
                return conn
            except sqlite3.OperationalError as exc:  # pragma: no cover - 竞态分支
                last_error = exc
                if "locked" not in str(exc).lower():
                    raise
                time.sleep(0.25)
        raise last_error  # pragma: no cover - 重试耗尽

    def _init_schema(self) -> None:
        # 双实例首次启动会并发建表/迁移：DDL 虽为 IF NOT EXISTS 幂等，
        # 但仍可能撞上写锁，按 busy 语义有限退避重试。
        last_error: Exception | None = None
        for attempt in range(60):
            try:
                with self._lock:
                    self._conn.executescript(_SCHEMA)
                    row = self._conn.execute(
                        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
                    ).fetchone()
                    if row is None:
                        self._conn.execute(
                            "INSERT INTO schema_meta(key, value) "
                            "VALUES ('schema_version', ?)",
                            (str(SCHEMA_VERSION),),
                        )
                        self._migrate_from_scratch()
                        return
                    current = int(row["value"])
                    if current == SCHEMA_VERSION:
                        return
                    if current > SCHEMA_VERSION:  # pragma: no cover - 迁移预留
                        raise RuntimeError(
                            f"数据库 schema 版本 {current} 新于程序 "
                            f"{SCHEMA_VERSION}，不兼容"
                        )
                    self._migrate_v1_to_v2()
                    self._conn.execute(
                        "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
                        (str(SCHEMA_VERSION),),
                    )
                    return
            except sqlite3.OperationalError as exc:  # pragma: no cover - 竞态分支
                last_error = exc
                if "locked" not in str(exc).lower():
                    raise
                time.sleep(0.25)
        raise last_error  # pragma: no cover - 重试耗尽

    def _migrate_from_scratch(self) -> None:
        """全新库：基础表已由 _SCHEMA 建好，无需回填。"""

    def _migrate_v1_to_v2(self) -> None:
        """v1 → v2：补代次列/代次表/目录表，并回填历史数据。

        幂等：重复执行不会破坏已迁移的数据（PRAGMA/INSERT OR IGNORE 保护）。
        """
        conn = self._conn
        if "seq" not in _column_names(conn, "receipts"):
            conn.execute("ALTER TABLE receipts ADD COLUMN seq INTEGER NOT NULL DEFAULT 1")
        # 历史接收：同号在 v1 只可能有一条，代次从 1 开始
        conn.execute(
            "INSERT OR IGNORE INTO receipt_generations(receipt_id, next_seq) "
            "SELECT receipt_id, 1 FROM receipts"
        )
        # 历史已发布接收：为每个站日回填一份 ACTIVE 目录（版本 1），
        # 保持升级前「下游按站日取最新已发布包」的可查询性
        conn.execute(
            "INSERT OR IGNORE INTO catalogs(station_id, obs_date, version, state, "
            "adopted_receipt_id, adopted_seq, adopted_sha256, invalid_reason) "
            "SELECT station_id, obs_date, 1, 'ACTIVE', receipt_id, seq, "
            "blob_sha256, NULL FROM receipts "
            "WHERE state = 'PUBLISHED' AND blob_sha256 IS NOT NULL"
        )

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """串行化写事务上下文。

        immediate=True 使用 BEGIN IMMEDIATE，进入即取写锁，
        配合 busy_timeout 实现跨进程写串行化。
        """
        with self._lock:
            if immediate:
                self._conn.execute("BEGIN IMMEDIATE")
            else:
                self._conn.execute("BEGIN")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params or ()).fetchone()

    def query_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params or ()).fetchall())

    def close(self) -> None:
        with self._lock:
            self._conn.close()
