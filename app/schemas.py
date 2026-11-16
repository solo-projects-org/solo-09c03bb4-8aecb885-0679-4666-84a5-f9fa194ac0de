"""请求/响应模型与字段校验。"""

from __future__ import annotations

import datetime as dt
import re

from pydantic import BaseModel, Field, field_validator

_RECEIPT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,63}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{64}$")


class DeclarationIn(BaseModel):
    """接收声明：站号、UTC 日期、原包长度与 SHA-256。"""

    receipt_id: str = Field(..., description="接收号，同号重传沿用同一记录")
    station_id: str = Field(..., min_length=1, max_length=64, description="浮标站号")
    obs_date: dt.date = Field(..., description="观测 UTC 日期，ISO 格式 YYYY-MM-DD")
    size: int = Field(..., ge=0, description="原包字节长度（Parquet 文件大小）")
    sha256: str = Field(..., min_length=64, max_length=64, description="原包 SHA-256 十六进制摘要")

    @field_validator("receipt_id")
    @classmethod
    def _check_receipt(cls, v: str) -> str:
        if not _RECEIPT_RE.match(v):
            raise ValueError("接收号仅允许字母数字及 ._- 且以字母数字开头，最长 64 位")
        return v

    @field_validator("sha256")
    @classmethod
    def _check_sha(cls, v: str) -> str:
        if not _SHA_RE.match(v):
            raise ValueError("SHA-256 必须是 64 位十六进制字符串")
        return v.lower()


class ReceiptOut(BaseModel):
    receipt_id: str
    station_id: str
    obs_date: str
    declared_size: int
    declared_sha256: str
    state: str
    bytes_received: int
    actual_sha256: str | None
    blob_sha256: str | None
    row_count: int | None
    last_error: str | None
    created_at: str
    updated_at: str


class UploadResult(BaseModel):
    receipt_id: str
    state: str
    bytes_received: int
    declared_size: int
    actual_sha256: str | None
    row_count: int | None
    message: str


class QuotaOut(BaseModel):
    quota_bytes: int
    used_bytes: int
    blob_bytes: int
    staging_bytes: int
    available_bytes: int


class ReconcileOut(BaseModel):
    removed_staging_files: int
    removed_orphan_blobs: int
    reset_receipts: int


# ---------------------------------------------------------------------------
# 站日目录 / 采纳
# ---------------------------------------------------------------------------

class CandidateOut(BaseModel):
    """站日候选接收：接收号、摘要与行数（仅当前仍已发布）。"""

    receipt_id: str
    seq: int = Field(..., description="入湖代次；同号删除后重新入湖代次递增")
    station_id: str
    obs_date: str
    declared_size: int
    declared_sha256: str
    blob_sha256: str | None
    row_count: int | None
    created_at: str
    updated_at: str


class CandidateListOut(BaseModel):
    station_id: str
    obs_date: str
    version: int = Field(..., description="查询时刻的目录版本，采纳时须原样回传")
    catalog_state: str = Field(..., description="VACANT / ACTIVE / INVALID")
    candidates: list[CandidateOut]


class CatalogOut(BaseModel):
    station_id: str
    obs_date: str
    version: int
    state: str = Field(..., description="VACANT 未采纳 / ACTIVE 有效 / INVALID 失效待重选")
    adopted_receipt_id: str | None
    adopted_seq: int | None
    adopted_sha256: str | None
    invalid_reason: str | None
    updated_at: str | None


class AdoptIn(BaseModel):
    """采纳请求：目标接收号 + 查询候选/目录时拿到的版本号（乐观并发）。"""

    receipt_id: str = Field(..., min_length=1, max_length=64, description="要采纳为正式原包的接收号")
    expected_version: int = Field(..., ge=0, description="查询时的目录版本；过期版本提交将被 409 拒绝")
