"""站日正式目录与采纳（adoption）领域逻辑。

每个「站 + UTC 日期」维护一行目录（catalogs 表）：

- ``version`` 单调递增的目录版本号。候选集变化（新接收发布）或正式选择变化
  （采纳、失效）都会让版本 +1；下游采纳时必须回传查询时拿到的版本号，
  旧版本提交一律 409 拒绝且不改变当前选择（乐观并发控制）。
- ``state``：
  - ``VACANT``  还没有正式原包；
  - ``ACTIVE``  已指定正式原包且当前有效；
  - ``INVALID`` 正式接收被删除、或对账发现其共享原件缺失；绝不自动改选，
    只能由分析员显式重新采纳。
- 采纳目标记录 ``adopted_receipt_id`` + ``adopted_seq`` + ``adopted_sha256``。
  同号接收删除后重新入湖，seq 代次 +1，即使 sha256 摘要完全相同，
  旧采纳也不会复活——必须重新采纳。

所有变更都在调用方的 BEGIN IMMEDIATE 写事务内执行（或本模块自建事务），
配合 WAL + busy_timeout 在双实例间串行化；进程内不缓存任何目录状态，
因此重启后读到的永远是共享存储上的最新事实。
"""

from __future__ import annotations

import os

from .db import Database
from .errors import (
    AdoptNotPublished,
    CatalogVersionConflict,
)
from .storage import blob_path

VACANT = "VACANT"
ACTIVE = "ACTIVE"
INVALID = "INVALID"

_NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def allocate_generation(conn, receipt_id: str) -> int:
    """登记一次（可能的重新）入湖，返回该接收号本次使用的代次。

    首次声明返回 1；删除后再次声明返回 2、3 ……单调递增。
    必须在写事务内调用。
    """
    row = conn.execute(
        "SELECT next_seq FROM receipt_generations WHERE receipt_id = ?",
        (receipt_id,),
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO receipt_generations(receipt_id, next_seq) VALUES (?, 2)",
            (receipt_id,),
        )
        return 1
    seq = int(row["next_seq"])
    conn.execute(
        "UPDATE receipt_generations SET next_seq = ? WHERE receipt_id = ?",
        (seq + 1, receipt_id),
    )
    return seq


def ensure_catalog_row(conn, station_id: str, obs_date: str) -> None:
    """确保站日目录行存在（惰性创建，初始 VACANT / version=0）。"""
    conn.execute(
        "INSERT OR IGNORE INTO catalogs(station_id, obs_date) VALUES (?, ?)",
        (station_id, obs_date),
    )


def bump_catalog_version(conn, station_id: str, obs_date: str) -> int:
    """候选/正式选择发生变化：确保目录行存在并把版本号 +1，返回新版本。"""
    ensure_catalog_row(conn, station_id, obs_date)
    conn.execute(
        f"UPDATE catalogs SET version = version + 1, updated_at = {_NOW} "
        "WHERE station_id = ? AND obs_date = ?",
        (station_id, obs_date),
    )
    row = conn.execute(
        "SELECT version FROM catalogs WHERE station_id = ? AND obs_date = ?",
        (station_id, obs_date),
    ).fetchone()
    return int(row["version"])


def mark_catalog_invalid(
    conn,
    station_id: str,
    obs_date: str,
    reason: str,
) -> None:
    """把站日目录标记失效（保留原采纳快照，不自动改选），版本 +1。

    幂等：目录已经是 INVALID 时不重复递增版本（连续两轮对账结果稳定）。
    """
    ensure_catalog_row(conn, station_id, obs_date)
    row = conn.execute(
        "SELECT state FROM catalogs WHERE station_id = ? AND obs_date = ?",
        (station_id, obs_date),
    ).fetchone()
    if row is not None and row["state"] == INVALID:
        # 已失效：只刷新原因/时间，版本保持稳定，避免对账空转放大版本号
        conn.execute(
            f"UPDATE catalogs SET invalid_reason = ?, updated_at = {_NOW} "
            "WHERE station_id = ? AND obs_date = ?",
            (reason, station_id, obs_date),
        )
        return
    conn.execute(
        f"UPDATE catalogs SET state = 'INVALID', invalid_reason = ?, "
        f"version = version + 1, updated_at = {_NOW} "
        "WHERE station_id = ? AND obs_date = ?",
        (reason, station_id, obs_date),
    )


def list_candidates(db: Database, station_id: str, obs_date: str) -> list[dict]:
    """站日候选：当前仍已发布的接收，显示接收号、摘要与行数（含代次）。

    只有 PUBLISHED 且持有 blob 引用的接收才是候选；被删除或被对账回退的
    接收自动从候选集消失。
    """
    rows = db.query_all(
        "SELECT receipt_id, seq, station_id, obs_date, declared_size, "
        "declared_sha256, blob_sha256, row_count, created_at, updated_at "
        "FROM receipts WHERE station_id = ? AND obs_date = ? "
        "AND state = 'PUBLISHED' AND blob_sha256 IS NOT NULL "
        "ORDER BY created_at ASC, receipt_id ASC",
        (station_id, obs_date),
    )
    return [dict(r) for r in rows]


def _get_catalog_row(db: Database, station_id: str, obs_date: str):
    return db.query_one(
        "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
        (station_id, obs_date),
    )


def _catalog_snapshot(row) -> dict:
    if row is None:
        return {
            "station_id": None,
            "obs_date": None,
            "version": 0,
            "state": VACANT,
            "adopted_receipt_id": None,
            "adopted_seq": None,
            "adopted_sha256": None,
            "invalid_reason": None,
            "updated_at": None,
        }
    return dict(row)


def get_catalog(db: Database, settings, station_id: str, obs_date: str) -> dict:
    """查询正式目录；读取时顺带做一次「失效自愈」核对。

    若 ACTIVE 目录的正式接收已不存在、代次已变（同号重新入湖）、不再是
    PUBLISHED，或共享原件在磁盘上缺失，则当场标记 INVALID（版本 +1），
    保证任何实例重启/交错之后都不会把失效接收继续标为有效。
    """
    row = _get_catalog_row(db, station_id, obs_date)
    if row is None or row["state"] != ACTIVE:
        return _catalog_snapshot(row)

    receipt = db.query_one(
        "SELECT * FROM receipts WHERE receipt_id = ?",
        (row["adopted_receipt_id"],),
    )
    invalid_reason = _check_adopted_health(settings, row, receipt)
    if invalid_reason is None:
        return _catalog_snapshot(row)

    with db.transaction() as conn:
        # 事务内重读，避免与并发采纳/删除竞争后误判
        latest = conn.execute(
            "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
            (station_id, obs_date),
        ).fetchone()
        if latest is None or latest["state"] != ACTIVE:
            return _catalog_snapshot(latest)
        receipt = conn.execute(
            "SELECT * FROM receipts WHERE receipt_id = ?",
            (latest["adopted_receipt_id"],),
        ).fetchone()
        if _check_adopted_health(settings, latest, receipt) is None:
            return _catalog_snapshot(latest)
        mark_catalog_invalid(conn, station_id, obs_date, invalid_reason)
        return _catalog_snapshot(
            conn.execute(
                "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
                (station_id, obs_date),
            ).fetchone()
        )


def _check_adopted_health(settings, catalog_row, receipt_row) -> str | None:
    """返回 None 表示正式接收仍然有效；否则返回失效原因。"""
    rid = catalog_row["adopted_receipt_id"]
    if receipt_row is None:
        return f"正式接收 {rid} 已被删除，目录失效，需重新采纳"
    if int(receipt_row["seq"]) != int(catalog_row["adopted_seq"]):
        return (
            f"正式接收号 {rid} 已重新入湖（代次 {catalog_row['adopted_seq']} → "
            f"{receipt_row['seq']}），旧采纳失效，需重新采纳"
        )
    if receipt_row["state"] != "PUBLISHED" or not receipt_row["blob_sha256"]:
        return f"正式接收 {rid} 已不再是已发布状态，目录失效，需重新采纳"
    if not os.path.isfile(blob_path(settings, receipt_row["blob_sha256"])):
        return (
            f"正式接收 {rid} 的共享原件（sha256={receipt_row['blob_sha256']}）"
            "在存储中缺失，目录失效，需重新采纳"
        )
    return None


def adopt(
    db: Database,
    settings,
    station_id: str,
    obs_date: str,
    receipt_id: str,
    expected_version: int,
) -> dict:
    """采纳一个接收为本站日正式原包（乐观并发 + 严格前置校验）。

    - 只能采纳本站日当前仍 PUBLISHED 的接收；
    - expected_version 必须等于提交时刻的目录版本，否则 409 且不改选择；
    - 已 INVALID 的目录只能通过显式采纳恢复为 ACTIVE，绝不自动改选；
    - 同号重新入湖（seq 变化）后旧采纳已失效，即使摘要相同也须重新采纳。
    """
    with db.transaction() as conn:
        ensure_catalog_row(conn, station_id, obs_date)
        catalog = conn.execute(
            "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
            (station_id, obs_date),
        ).fetchone()
        if int(catalog["version"]) != int(expected_version):
            raise CatalogVersionConflict(
                f"目录版本已变化：查询时版本 {expected_version}，"
                f"当前版本 {catalog['version']}；请重新查询候选与版本后再采纳",
            )

        receipt = conn.execute(
            "SELECT * FROM receipts WHERE receipt_id = ?",
            (receipt_id,),
        ).fetchone()
        if receipt is None:
            raise AdoptNotPublished(
                f"接收 {receipt_id} 不存在或已被删除，无法采纳为正式原包"
            )
        if (
            receipt["station_id"] != station_id
            or receipt["obs_date"] != obs_date
        ):
            raise AdoptNotPublished(
                f"接收 {receipt_id} 属于站日 "
                f"{receipt['station_id']}/{receipt['obs_date']}，"
                f"不属于 {station_id}/{obs_date}，不能采纳"
            )
        if receipt["state"] != "PUBLISHED" or not receipt["blob_sha256"]:
            raise AdoptNotPublished(
                f"接收 {receipt_id} 当前不是已发布状态，不能采纳为正式原包"
            )
        if not os.path.isfile(blob_path(settings, receipt["blob_sha256"])):
            # 共享原件缺失：本次采纳直接拒绝并把目录（若原本有效）标失效
            reason = (
                f"接收 {receipt_id} 的共享原件"
                f"（sha256={receipt['blob_sha256']}）缺失，不能采纳"
            )
            if catalog["state"] == ACTIVE:
                mark_catalog_invalid(conn, station_id, obs_date, reason)
            raise AdoptNotPublished(reason)

        # 重复采纳同一代次：幂等成功，不重复递增版本
        if (
            catalog["state"] == ACTIVE
            and catalog["adopted_receipt_id"] == receipt_id
            and int(catalog["adopted_seq"]) == int(receipt["seq"])
        ):
            return _catalog_snapshot(
                conn.execute(
                    "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
                    (station_id, obs_date),
                ).fetchone()
            )

        conn.execute(
            f"UPDATE catalogs SET state = 'ACTIVE', adopted_receipt_id = ?, "
            "adopted_seq = ?, adopted_sha256 = ?, invalid_reason = NULL, "
            f"version = version + 1, updated_at = {_NOW} "
            "WHERE station_id = ? AND obs_date = ?",
            (
                receipt_id,
                receipt["seq"],
                receipt["blob_sha256"],
                station_id,
                obs_date,
            ),
        )
        return _catalog_snapshot(
            conn.execute(
                "SELECT * FROM catalogs WHERE station_id = ? AND obs_date = ?",
                (station_id, obs_date),
            ).fetchone()
        )
