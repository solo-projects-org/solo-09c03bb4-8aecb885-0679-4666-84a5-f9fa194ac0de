"""领域异常与 HTTP 状态映射。"""

from __future__ import annotations


class ObsError(Exception):
    """所有可预期业务错误的基类。"""

    status_code = 400
    code = "bad_request"

    def __init__(self, message: str, *, code: str | None = None, status_code: int | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


class ReceiptNotFound(ObsError):
    status_code = 404
    code = "receipt_not_found"


class DeclarationConflict(ObsError):
    status_code = 409
    code = "declaration_conflict"


class ReceiptBusy(ObsError):
    status_code = 409
    code = "receipt_busy"


class QuotaExceeded(ObsError):
    status_code = 507
    code = "quota_exceeded"


class UploadNotComplete(ObsError):
    status_code = 409
    code = "upload_not_complete"


class ContentMismatch(ObsError):
    status_code = 422
    code = "content_mismatch"


class InvalidPackage(ObsError):
    status_code = 422
    code = "invalid_package"


class AlreadyFinalized(ObsError):
    status_code = 409
    code = "already_finalized"


class CatalogVersionConflict(ObsError):
    """提交时携带的目录版本与当前版本不一致：候选或正式选择已变化。"""

    status_code = 409
    code = "catalog_version_conflict"


class CatalogNotFound(ObsError):
    status_code = 404
    code = "catalog_not_found"


class AdoptNotPublished(ObsError):
    """采纳目标接收在本站日已不再是已发布状态（被删除或回退）。"""

    status_code = 409
    code = "adopt_not_published"


class CatalogInvalid(ObsError):
    """目录已失效（正式接收被删除或共享原件缺失），只能重新采纳，不能自动改选。"""

    status_code = 409
    code = "catalog_invalid"
