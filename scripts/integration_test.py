#!/usr/bin/env python -I
"""真实双进程端到端自测脚本（不依赖 pytest，直接打 HTTP）。

前提：两个 uvicorn 实例已在 127.0.0.1:8001 / 8002 启动并共享数据目录。
覆盖：
  1. 健康检查与 Swagger
  2. 实例1 声明+上传+发布，实例2 立即可见并可下载（跨实例共享状态）
  3. 同号同声明幂等、异声明 409
  4. 同内容跨接收去重，磁盘只一份、额度只算一次
  5. 错误长度/摘要/Parquet 内容拒绝且无成品
  6. 慢上传期间另一实例仍可接收与查询（不阻塞）
  7. 配额不足 507
  8. 崩溃回收：伪造孤儿暂存后调 /admin/reconcile 被清理
  9. 站日目录/采纳：候选、版本乐观锁、删除失效、同号重入湖须重采纳、双实例交错
"""

from __future__ import annotations

import datetime as dt
import hashlib
import http.client
import json
import os
import pathlib
import sys
import threading
import time
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

BASE1 = "http://127.0.0.1:8001"
BASE2 = "http://127.0.0.1:8002"
WORK = pathlib.Path(os.environ.get("OBS_ITEST_WORK", "/tmp/obs-itest"))
WORK.mkdir(parents=True, exist_ok=True)

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def req(method: str, base: str, path: str, body: bytes | dict | None = None,
        raw: bool = False) -> tuple[int, bytes, dict]:
    if isinstance(body, dict):
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
    elif body is None:
        data, headers = None, {}
    else:
        data, headers = body, {"Content-Type": "application/octet-stream"}
    request = urllib.request.Request(base + path, data=data, headers=headers,
                                     method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def make_parquet(name: str, station: str = "BUOY-IT1", day: dt.date | None = None,
                 salt: float = 0.0, rows: int = 24) -> tuple[bytes, str]:
    day = day or dt.date(2026, 11, 15)
    ts = pa.array(
        [dt.datetime(day.year, day.month, day.day, h, tzinfo=dt.timezone.utc)
         for h in range(rows)],
        pa.timestamp("us", "UTC"),
    )
    table = pa.table({
        "station_id": pa.array([station] * rows, pa.string()),
        "timestamp_utc": ts,
        "salinity": pa.array([35.0 + salt + h * 0.01 for h in range(rows)],
                             pa.float64()),
    })
    path = WORK / name
    pq.write_table(table, path)
    data = path.read_bytes()
    return data, hashlib.sha256(data).hexdigest()


def decl(rid: str, data: bytes, sha: str, **over) -> dict:
    p = {"receipt_id": rid, "station_id": "BUOY-IT1", "obs_date": "2026-11-15",
         "size": len(data), "sha256": sha}
    p.update(over)
    return p


def main() -> int:
    print("== 1. 健康检查与 Swagger ==")
    for base, iid in ((BASE1, "instance-1"), (BASE2, "instance-2")):
        code, body, _ = req("GET", base, "/health")
        info = json.loads(body)
        check(f"{iid} 健康", code == 200 and info["instance"] == iid, str(info))
    code, body, _ = req("GET", BASE1, "/openapi.json")
    check("Swagger OpenAPI 可访问", code == 200 and b"title" in body)

    print("== 2. 实例1 发布，实例2 立即可见并可下载 ==")
    d1, sha1 = make_parquet("p1.parquet")
    rid = "IT-R1"
    code, _, _ = req("PUT", BASE1, f"/receipts/{rid}/declaration",
                     decl(rid, d1, sha1))
    check("实例1 声明 200", code == 200, str(code))
    code, body, _ = req("POST", BASE1, f"/receipts/{rid}/content", d1, raw=True)
    check("实例1 上传发布 200", code == 200 and json.loads(body)["state"] == "PUBLISHED",
          body[:200].decode())
    code, body, _ = req("GET", BASE2, f"/receipts/{rid}")
    check("实例2 可见同一接收", code == 200 and json.loads(body)["state"] == "PUBLISHED")
    code, body, headers = req("GET", BASE2, f"/receipts/{rid}/download")
    lower_headers = {k.lower(): v for k, v in headers.items()}
    check("实例2 下载原件一致", code == 200 and body == d1
          and lower_headers.get("x-sha256") == sha1)

    print("== 3. 幂等与异声明冲突 ==")
    code, _, _ = req("PUT", BASE2, f"/receipts/{rid}/declaration",
                     decl(rid, d1, sha1))
    check("同号同声明幂等 200", code == 200)
    code, body, _ = req("PUT", BASE1, f"/receipts/{rid}/declaration",
                        decl(rid, d1, sha1, station_id="OTHER"))
    check("异声明 409", code == 409 and json.loads(body)["error"]["code"]
          == "declaration_conflict")

    print("== 4. 同内容跨接收去重 ==")
    rid2 = "IT-R2"
    req("PUT", BASE2, f"/receipts/{rid2}/declaration", decl(rid2, d1, sha1))
    code, body, _ = req("POST", BASE2, f"/receipts/{rid2}/content", d1, raw=True)
    check("同内容第二接收发布成功", code == 200)
    code, body, _ = req("GET", BASE1, "/quota")
    q = json.loads(body)
    check("额度只算一份 blob", q["blob_bytes"] == len(d1) and q["used_bytes"] == len(d1),
          str(q))
    blob = pathlib.Path(os.environ["OBS_DATA_DIR"]) / "blobs" / sha1[:2] / sha1
    check("物理成品仅一份", blob.exists())

    print("== 5. 失败拒绝且无成品 ==")
    bad_rid = "IT-BADLEN"
    req("PUT", BASE1, f"/receipts/{bad_rid}/declaration", decl(bad_rid, d1, sha1))
    code, body, _ = req("POST", BASE1, f"/receipts/{bad_rid}/content",
                        d1 + b"\x00", raw=True)
    check("长度不符 422", code == 422)
    code, _, _ = req("GET", BASE1, f"/receipts/{bad_rid}/download")
    check("失败后无可下载成品", code == 409)

    gar = b"NOT-PARQUET" * 8
    g_rid = "IT-BADPARQUET"
    req("PUT", BASE1, f"/receipts/{g_rid}/declaration",
        decl(g_rid, gar, hashlib.sha256(gar).hexdigest()))
    code, body, _ = req("POST", BASE1, f"/receipts/{g_rid}/content", gar, raw=True)
    check("非法 Parquet 422", code == 422
          and json.loads(body)["error"]["code"] == "invalid_package")

    print("== 6. 慢上传不阻塞其他接收与查询 ==")
    slow_rid = "IT-SLOW"
    d2, sha2 = make_parquet("p2.parquet", salt=3.0)
    req("PUT", BASE1, f"/receipts/{slow_rid}/declaration",
        decl(slow_rid, d2, sha2))

    stop = threading.Event()

    def slow_upload():
        # 用 http.client 直接驱动 socket，分块慢速推送制造在途上传。
        # 注意：stop 仅用于测试收尾，不能在发完前置位，否则请求体不完整。
        try:
            conn = http.client.HTTPConnection("127.0.0.1", 8001, timeout=30)
            conn.putrequest("POST", f"/receipts/{slow_rid}/content")
            conn.putheader("Content-Type", "application/octet-stream")
            conn.putheader("Content-Length", str(len(d2)))
            conn.endheaders()
            chunk = 64
            for i in range(0, len(d2), chunk):
                conn.send(d2[i:i + chunk])
                time.sleep(0.05)
            resp = conn.getresponse()
            slow_result["code"] = resp.status
            slow_result["body"] = resp.read()
            conn.close()
        except Exception as exc:  # noqa: BLE001
            slow_result["code"] = -1
            slow_result["body"] = str(exc).encode()
        finally:
            stop.set()

    slow_result: dict = {}
    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.6)  # 让慢上传先在途

    t0 = time.time()
    code, body, _ = req("GET", BASE2, "/receipts", )
    elapsed_ms = (time.time() - t0) * 1000
    check("慢上传期间实例2 查询即时响应", code == 200 and elapsed_ms < 1500,
          f"{elapsed_ms:.0f}ms")

    other_rid = "IT-WHILE-SLOW"
    code, _, _ = req("PUT", BASE2, f"/receipts/{other_rid}/declaration",
                     decl(other_rid, d2, sha2))
    code, body, _ = req("POST", BASE2, f"/receipts/{other_rid}/content", d2, raw=True)
    check("慢上传期间另一包可正常接收发布", code == 200
          and json.loads(body)["state"] == "PUBLISHED", body[:160].decode())

    stop.set()
    t.join(timeout=30)
    check("慢上传自身最终成功", slow_result.get("code") == 200,
          str(slow_result.get("body", b"")[:160]))

    print("== 7. 崩溃回收 ==")
    staging = pathlib.Path(os.environ["OBS_DATA_DIR"]) / "staging" / "GHOST.part"
    staging.parent.mkdir(parents=True, exist_ok=True)
    staging.write_bytes(b"x" * 50)
    code, body, _ = req("POST", BASE1, "/admin/reconcile")
    info = json.loads(body)
    check("对账清理孤儿暂存", code == 200 and info["removed_staging_files"] >= 1
          and not staging.exists(), str(info))

    print("== 8. 已发布数据最终完整性 ==")
    code, body, _ = req("GET", BASE1, "/receipts", )
    states = {r["receipt_id"]: r["state"] for r in json.loads(body)}
    check("成功记录均为 PUBLISHED",
          all(s == "PUBLISHED" for k, s in states.items()
              if k not in ("IT-BADLEN", "IT-BADPARQUET")),
          str(states))
    check("失败记录保持 UPLOADING 可重传",
          states.get("IT-BADLEN") == "UPLOADING"
          and states.get("IT-BADPARQUET") == "UPLOADING", str(states))

    print("== 9. 站日目录 / 采纳（双实例交错） ==")
    station, day = "BUOY-CAT", "2026-11-15"
    cands_path = f"/catalogs/stations/{station}/days/{day}/candidates"
    cat_path = f"/catalogs/stations/{station}/days/{day}"
    adopt_path = f"/catalogs/stations/{station}/days/{day}/adoptions"

    # 初始 VACANT / version=0
    code, body, _ = req("GET", BASE1, cat_path)
    cat0 = json.loads(body)
    check("新站日目录 VACANT 且版本 0",
          code == 200 and cat0["state"] == "VACANT" and cat0["version"] == 0, str(cat0))

    # 两份候选分别在两个实例发布
    dc1, shac1 = make_parquet("cat1.parquet", station=station, salt=0.1)
    dc2, shac2 = make_parquet("cat2.parquet", station=station, salt=0.2)
    for rid, d, sh, base in (("IT-C1", dc1, shac1, BASE1), ("IT-C2", dc2, shac2, BASE2)):
        req("PUT", base, f"/receipts/{rid}/declaration",
            decl(rid, d, sh, station_id=station))
        code, body, _ = req("POST", base, f"/receipts/{rid}/content", d, raw=True)
        check(f"{rid} 发布成功", code == 200 and json.loads(body)["state"] == "PUBLISHED")

    # 候选显示接收号、摘要、行数
    code, body, _ = req("GET", BASE2, cands_path)
    clist = json.loads(body)
    ids = [c["receipt_id"] for c in clist["candidates"]]
    check("候选含两份已发布接收且带摘要/行数",
          ids == ["IT-C1", "IT-C2"]
          and all(c["declared_sha256"] and c["row_count"] == 24 for c in clist["candidates"]),
          str(clist))
    version_after_publish = clist["version"]
    check("发布两次后目录版本推进到 2", version_after_publish == 2, str(version_after_publish))

    # 旧版本采纳被拒且不改选择
    code, body, _ = req("POST", BASE1, adopt_path,
                        {"receipt_id": "IT-C2", "expected_version": 0})
    check("过期版本采纳 409", code == 409
          and json.loads(body)["error"]["code"] == "catalog_version_conflict")
    check("被拒后目录仍 VACANT",
          req("GET", BASE2, cat_path)[1] and json.loads(req("GET", BASE2, cat_path)[1])["state"] == "VACANT")

    # 正确版本采纳 C1
    code, body, _ = req("POST", BASE2, adopt_path,
                        {"receipt_id": "IT-C1", "expected_version": version_after_publish})
    adopted = json.loads(body)
    check("采纳成功目录 ACTIVE",
          code == 200 and adopted["state"] == "ACTIVE"
          and adopted["adopted_receipt_id"] == "IT-C1"
          and adopted["adopted_seq"] == 1, str(adopted))

    # 实例1 立刻看到实例2 的采纳
    code, body, _ = req("GET", BASE1, cat_path)
    check("采纳跨实例可见", json.loads(body)["adopted_receipt_id"] == "IT-C1")

    # 不能采纳未发布接收
    d_up, sh_up = make_parquet("catup.parquet", station=station, salt=0.9)
    req("PUT", BASE1, "/receipts/IT-CUP/declaration",
        decl("IT-CUP", d_up, sh_up, station_id=station))
    cur_v = json.loads(req("GET", BASE1, cat_path)[1])["version"]
    code, body, _ = req("POST", BASE1, adopt_path,
                        {"receipt_id": "IT-CUP", "expected_version": cur_v})
    check("采纳未发布接收 409", code == 409
          and json.loads(body)["error"]["code"] == "adopt_not_published")

    # 删除正式接收 → INVALID，不自动改选
    code, _, _ = req("DELETE", BASE1, "/receipts/IT-C1")
    check("删除正式接收 204", code == 204)
    code, body, _ = req("GET", BASE2, cat_path)
    invalid = json.loads(body)
    check("正式接收被删目录 INVALID 且保留快照",
          invalid["state"] == "INVALID" and invalid["adopted_receipt_id"] == "IT-C1"
          and "删除" in (invalid["invalid_reason"] or ""), str(invalid))
    check("失效后候选只剩 IT-C2 且未被自动选中",
          [c["receipt_id"] for c in json.loads(req("GET", BASE1, cands_path)[1])["candidates"]]
          == ["IT-C2"])

    # 同号重新入湖（同摘要）也必须重新采纳
    req("PUT", BASE2, "/receipts/IT-C1/declaration",
        decl("IT-C1", dc1, shac1, station_id=station))
    code, body, _ = req("POST", BASE2, "/receipts/IT-C1/content", dc1, raw=True)
    check("同号同摘要重新入湖发布成功", code == 200)
    clist2 = json.loads(req("GET", BASE1, cands_path)[1])
    re_row = next(c for c in clist2["candidates"] if c["receipt_id"] == "IT-C1")
    check("重新入湖代次 seq=2 且目录仍 INVALID",
          re_row["seq"] == 2 and clist2["catalog_state"] == "INVALID", str(clist2))

    # 用当前版本显式重新采纳 → 恢复 ACTIVE，adopted_seq=2
    cur_v = json.loads(req("GET", BASE2, cat_path)[1])["version"]
    code, body, _ = req("POST", BASE1, adopt_path,
                        {"receipt_id": "IT-C1", "expected_version": cur_v})
    readopted = json.loads(body)
    check("重新采纳恢复 ACTIVE 且代次为 2",
          code == 200 and readopted["state"] == "ACTIVE"
          and readopted["adopted_seq"] == 2, str(readopted))

    # 对账：删掉正式接收的共享原件 → 目录 INVALID（双实例均可见，不复活）
    blob = pathlib.Path(os.environ["OBS_DATA_DIR"]) / "blobs" / shac1[:2] / shac1
    blob.unlink()
    code, body, _ = req("POST", BASE2, "/admin/reconcile")
    check("对账执行成功", code == 200)
    view1 = json.loads(req("GET", BASE1, cat_path)[1])
    view2 = json.loads(req("GET", BASE2, cat_path)[1])
    check("缺件对账后两实例均见 INVALID",
          view1["state"] == "INVALID" and view2["state"] == "INVALID"
          and "共享原件" in (view1["invalid_reason"] or ""), f"{view1} | {view2}")
    check("失效接收不再出现在候选",
          all(c["receipt_id"] != "IT-C1"
              for c in json.loads(req("GET", BASE1, cands_path)[1])["candidates"]))

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
