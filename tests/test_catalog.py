"""站日目录与采纳（adoption）测试。

覆盖需求关键点：
- 候选目录显示接收号、摘要、行数，只含本站日仍已发布的接收；
- 采纳只能选本站日仍已发布的接收，且必须携带查询时目录版本；
- 候选/采纳变化后旧版本提交被拒绝且不改当前选择；
- 已采纳接收被删除、或对账发现共享原件缺失 → 目录 INVALID，不自动改选；
- 同号重新入湖即使摘要相同也须重新采纳（seq 代次）；
- 双实例（双 Database 连接）采纳/删除/对账交错，以及重启后，
  目录不得把失效接收标为有效。
"""

from __future__ import annotations

import pathlib
import threading

from conftest import declare_payload, make_parquet

from app import main as main_mod
from app import catalog as cat
from app.db import Database
from app.storage import blob_path, reconcile


STATION = "BUOY-CAT"
DAY = "2026-11-15"
CANDIDATES = f"/catalogs/stations/{STATION}/days/{DAY}/candidates"
CATALOG = f"/catalogs/stations/{STATION}/days/{DAY}"
ADOPTIONS = f"/catalogs/stations/{STATION}/days/{DAY}/adoptions"


def _publish(client, rid: str, data: bytes, *, station: str = STATION, day: str = DAY):
    r = client.put(
        f"/receipts/{rid}/declaration",
        json=declare_payload(rid, data, station_id=station, obs_date=day),
    )
    assert r.status_code == 200, r.text
    r = client.post(f"/receipts/{rid}/content", content=data)
    assert r.status_code == 200, r.text
    return r


def _adopt(client, rid: str, version: int):
    return client.post(ADOPTIONS, json={"receipt_id": rid, "expected_version": version})


def test_catalog_starts_vacant_with_version_zero(client, tmp_path, helpers):
    r = client.get(CANDIDATES)
    assert r.status_code == 200
    body = r.json()
    assert body["candidates"] == []
    assert body["version"] == 0
    assert body["catalog_state"] == "VACANT"

    r = client.get(CATALOG)
    assert r.status_code == 200
    assert r.json()["state"] == "VACANT"
    assert r.json()["version"] == 0
    assert r.json()["adopted_receipt_id"] is None


def test_candidates_show_receipt_id_digest_and_row_count(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION, rows=24)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, rows=12, salt=1.0)
    _publish(client, "CAT-R1", d1)
    _publish(client, "CAT-R2", d2)

    r = client.get(CANDIDATES)
    assert r.status_code == 200
    body = r.json()
    assert body["catalog_state"] == "VACANT"
    # 两次发布各推一版：version=2
    assert body["version"] == 2
    ids = [c["receipt_id"] for c in body["candidates"]]
    assert ids == ["CAT-R1", "CAT-R2"]  # 按创建时间排序
    first = body["candidates"][0]
    assert first["declared_sha256"] == helpers.sha256_of(d1)
    assert first["row_count"] == 24
    assert first["seq"] == 1
    assert body["candidates"][1]["row_count"] == 12


def test_candidates_only_published_and_only_same_station_day(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-SAME", d1)
    # 另一站日的发布不得出现在候选里
    d_other = make_parquet(tmp_path / "other.parquet", station_id="BUOY-OTHER")
    _publish(client, "CAT-OTHER", d_other, station="BUOY-OTHER", day=DAY)
    # 本站日未发布（UPLOADING）的接收不得出现
    d_up = make_parquet(tmp_path / "up.parquet", station_id=STATION, salt=2.0)
    client.put(
        "/receipts/CAT-UP/declaration",
        json=declare_payload("CAT-UP", d_up, station_id=STATION, obs_date=DAY),
    )

    body = client.get(CANDIDATES).json()
    assert [c["receipt_id"] for c in body["candidates"]] == ["CAT-SAME"]


def test_successful_adoption_makes_catalog_active(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-A1", d1)
    version = client.get(CANDIDATES).json()["version"]

    r = _adopt(client, "CAT-A1", version)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["state"] == "ACTIVE"
    assert out["adopted_receipt_id"] == "CAT-A1"
    assert out["adopted_seq"] == 1
    assert out["adopted_sha256"] == helpers.sha256_of(d1)
    assert out["version"] == version + 1
    assert out["invalid_reason"] is None


def test_adoption_requires_current_version_and_keeps_choice_on_stale(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=1.0)
    _publish(client, "CAT-V1", d1)
    stale_version = client.get(CANDIDATES).json()["version"]  # =1

    # 又一份包发布 → 候选变化、版本推进
    _publish(client, "CAT-V2", d2)
    current = client.get(CATALOG).json()
    assert current["version"] == stale_version + 1

    # 旧版本提交：409 且当前选择不变（仍 VACANT）
    r = _adopt(client, "CAT-V2", stale_version)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "catalog_version_conflict"
    assert client.get(CATALOG).json()["state"] == "VACANT"

    # 用新版本采纳成功
    r = _adopt(client, "CAT-V2", current["version"])
    assert r.status_code == 200
    assert r.json()["state"] == "ACTIVE"
    assert r.json()["adopted_receipt_id"] == "CAT-V2"

    # 采纳之后再用旧版本改选：同样被拒，选择保持 CAT-V2
    r = _adopt(client, "CAT-V1", current["version"])
    assert r.status_code == 409
    assert client.get(CATALOG).json()["adopted_receipt_id"] == "CAT-V2"


def test_cannot_adopt_unpublished_foreign_or_missing_receipt(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-PUB", d1)
    d_up = make_parquet(tmp_path / "up.parquet", station_id=STATION, salt=3.0)
    client.put(
        "/receipts/CAT-NP/declaration",
        json=declare_payload("CAT-NP", d_up, station_id=STATION, obs_date=DAY),
    )
    version = client.get(CANDIDATES).json()["version"]

    # 未发布接收
    r = _adopt(client, "CAT-NP", version)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "adopt_not_published"
    # 不存在的接收
    r = _adopt(client, "CAT-GHOST", version)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "adopt_not_published"
    # 别的站日的接收
    d_other = make_parquet(tmp_path / "o.parquet", station_id="BUOY-X")
    _publish(client, "CAT-FOREIGN", d_other, station="BUOY-X", day=DAY)
    r = _adopt(client, "CAT-FOREIGN", version)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "adopt_not_published"
    # 目录未被这些失败请求改变
    assert client.get(CATALOG).json()["state"] == "VACANT"


def test_adoption_is_idempotent_for_same_generation(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-IDEM", d1)
    version = client.get(CANDIDATES).json()["version"]
    r1 = _adopt(client, "CAT-IDEM", version)
    assert r1.status_code == 200
    adopted_version = r1.json()["version"]

    # 同代次重复采纳（带最新版本）：幂等，版本不再递增
    r2 = _adopt(client, "CAT-IDEM", adopted_version)
    assert r2.status_code == 200
    assert r2.json()["version"] == adopted_version
    assert r2.json()["adopted_receipt_id"] == "CAT-IDEM"


def test_deleting_adopted_receipt_invalidates_catalog_no_auto_reswitch(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=1.0)
    _publish(client, "CAT-D1", d1)
    _publish(client, "CAT-D2", d2)
    version = client.get(CANDIDATES).json()["version"]
    r = _adopt(client, "CAT-D1", version)
    assert r.status_code == 200
    assert r.json()["state"] == "ACTIVE"

    # 删除正式接收
    r = client.delete("/receipts/CAT-D1")
    assert r.status_code == 204

    out = client.get(CATALOG).json()
    assert out["state"] == "INVALID"
    assert out["adopted_receipt_id"] == "CAT-D1"  # 保留失效快照，不自动改选
    assert out["adopted_seq"] == 1
    assert "删除" in out["invalid_reason"]
    # 另一候选仍在候选目录里，但没有被自动选为正式包
    candidates = client.get(CANDIDATES).json()
    assert [c["receipt_id"] for c in candidates["candidates"]] == ["CAT-D2"]
    assert candidates["catalog_state"] == "INVALID"


def test_invalid_catalog_can_only_recover_by_explicit_readoption(client, tmp_path, helpers):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=1.0)
    _publish(client, "CAT-IR1", d1)
    _publish(client, "CAT-IR2", d2)
    _adopt(client, "CAT-IR1", client.get(CANDIDATES).json()["version"])
    client.delete("/receipts/CAT-IR1")
    assert client.get(CATALOG).json()["state"] == "INVALID"

    # 失效后旧版本号采纳必须被拒
    stale = client.get(CATALOG).json()["version"] - 1
    r = _adopt(client, "CAT-IR2", stale)
    assert r.status_code == 409

    # 显式用当前版本重新采纳另一接收 → 恢复 ACTIVE
    current_version = client.get(CATALOG).json()["version"]
    r = _adopt(client, "CAT-IR2", current_version)
    assert r.status_code == 200
    out = r.json()
    assert out["state"] == "ACTIVE"
    assert out["adopted_receipt_id"] == "CAT-IR2"
    assert out["invalid_reason"] is None


def test_reconcile_missing_blob_invalidates_adopted_catalog(
    client, settings, database, tmp_path, helpers
):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-M1", d1)
    sha = helpers.sha256_of(d1)
    _adopt(client, "CAT-M1", client.get(CANDIDATES).json()["version"])
    assert client.get(CATALOG).json()["state"] == "ACTIVE"

    # 模拟共享原件在磁盘上丢失（对账须发现并把目录置失效）
    blob = pathlib.Path(blob_path(settings, sha))
    blob.unlink()

    result = reconcile(settings, database)
    assert result["reset_receipts"] >= 1
    out = client.get(CATALOG).json()
    assert out["state"] == "INVALID"
    assert "共享原件" in out["invalid_reason"]
    # 失效接收不在候选中
    assert client.get(CANDIDATES).json()["candidates"] == []


def test_reconcile_missing_blob_of_non_adopted_candidate_bumps_version(
    client, settings, database, tmp_path, helpers
):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=4.0)
    _publish(client, "CAT-NB1", d1)
    _publish(client, "CAT-NB2", d2)
    # 正式选 NB2，NB1 只是候选
    _adopt(client, "CAT-NB2", client.get(CANDIDATES).json()["version"])
    before = client.get(CATALOG).json()["version"]

    sha1 = helpers.sha256_of(d1)
    pathlib.Path(blob_path(settings, sha1)).unlink()
    reconcile(settings, database)

    out = client.get(CATALOG).json()
    assert out["state"] == "ACTIVE"  # 正式包不受影响
    assert out["adopted_receipt_id"] == "CAT-NB2"
    assert out["version"] == before + 1  # 候选变化推版本
    assert [c["receipt_id"] for c in client.get(CANDIDATES).json()["candidates"]] == [
        "CAT-NB2"
    ]


def test_same_receipt_reingested_even_identical_digest_requires_readoption(
    client, tmp_path, helpers
):
    data = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-REPEAT", data)
    _adopt(client, "CAT-REPEAT", client.get(CANDIDATES).json()["version"])
    assert client.get(CATALOG).json()["state"] == "ACTIVE"

    # 删除后同号、同声明（摘要完全相同）重新入湖
    client.delete("/receipts/CAT-REPEAT")
    assert client.get(CATALOG).json()["state"] == "INVALID"
    _publish(client, "CAT-REPEAT", data)

    # 新接收代次为 2；目录不会因摘要相同而自动复活
    cand = client.get(CANDIDATES).json()
    assert len(cand["candidates"]) == 1
    assert cand["candidates"][0]["seq"] == 2
    assert cand["catalog_state"] == "INVALID"
    out = client.get(CATALOG).json()
    assert out["state"] == "INVALID"
    assert out["adopted_seq"] == 1  # 旧快照保留

    # 必须重新采纳才恢复
    r = _adopt(client, "CAT-REPEAT", cand["version"])
    assert r.status_code == 200
    recovered = r.json()
    assert recovered["state"] == "ACTIVE"
    assert recovered["adopted_seq"] == 2
    assert recovered["adopted_sha256"] == helpers.sha256_of(data)


def test_read_time_self_heal_marks_invalid_when_blob_disappears(
    client, settings, tmp_path, helpers
):
    """不走 reconcile，直接删磁盘文件后查询正式目录：读取即自愈标失效。"""
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-HEAL", d1)
    _adopt(client, "CAT-HEAL", client.get(CANDIDATES).json()["version"])
    sha = helpers.sha256_of(d1)
    pathlib.Path(blob_path(settings, sha)).unlink()

    out = client.get(CATALOG).json()
    assert out["state"] == "INVALID"
    assert out["adopted_receipt_id"] == "CAT-HEAL"
    # 再查一次保持 INVALID，版本不被反复放大
    v1 = out["version"]
    assert client.get(CATALOG).json()["version"] == v1


def test_two_database_instances_interleaved_adopt_delete_reconcile(
    client, settings, database, tmp_path, helpers
):
    """双实例交错：两个 Database 连接共享同一库，采纳/删除/对账交替进行。"""
    db_a = Database(settings.db_path)
    db_b = Database(settings.db_path)
    try:
        d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
        d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=7.0)
        _publish(client, "CAT-X1", d1)
        _publish(client, "CAT-X2", d2)

        # 实例 A 查候选并采纳
        version_a = cat.get_catalog(db_a, settings, STATION, DAY)["version"]
        out = cat.adopt(db_a, settings, STATION, DAY, "CAT-X1", version_a)
        assert out["state"] == cat.ACTIVE

        # 实例 B 立刻看到同一份正式目录
        seen_b = cat.get_catalog(db_b, settings, STATION, DAY)
        assert seen_b["state"] == cat.ACTIVE
        assert seen_b["adopted_receipt_id"] == "CAT-X1"

        # 实例 B 用过期版本改选 → 拒绝，选择不变
        stale = seen_b["version"] - 1
        try:
            cat.adopt(db_b, settings, STATION, DAY, "CAT-X2", stale)
            raise AssertionError("expected CatalogVersionConflict")
        except Exception as exc:
            assert exc.code == "catalog_version_conflict"
        assert cat.get_catalog(db_a, settings, STATION, DAY)["adopted_receipt_id"] == "CAT-X1"

        # 实例 B 删除正式接收 → 目录失效（两实例视角一致）
        from app.service import delete_receipt

        delete_receipt(settings, db_b, "CAT-X1")
        for dbx in (db_a, db_b):
            view = cat.get_catalog(dbx, settings, STATION, DAY)
            assert view["state"] == cat.INVALID
            assert view["adopted_receipt_id"] == "CAT-X1"

        # 实例 A 跑对账，不得把失效接收重新标为有效
        reconcile(settings, db_a)
        view = cat.get_catalog(db_b, settings, STATION, DAY)
        assert view["state"] == cat.INVALID

        # 实例 A 用当前版本重新采纳剩余候选 → 恢复
        current = cat.get_catalog(db_a, settings, STATION, DAY)["version"]
        recovered = cat.adopt(db_a, settings, STATION, DAY, "CAT-X2", current)
        assert recovered["state"] == cat.ACTIVE
        assert recovered["adopted_receipt_id"] == "CAT-X2"
        assert cat.get_catalog(db_b, settings, STATION, DAY)["state"] == cat.ACTIVE
    finally:
        db_a.close()
        db_b.close()


def test_restart_simulation_never_revives_invalid_receipt(
    client, settings, tmp_path, helpers
):
    """模拟重启：关闭连接后以全新 Database 重新打开同一数据文件。"""
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=9.0)
    _publish(client, "CAT-K1", d1)
    _publish(client, "CAT-K2", d2)
    _adopt(client, "CAT-K1", client.get(CANDIDATES).json()["version"])

    # 重启前令正式接收原件缺失并对账失效
    sha1 = helpers.sha256_of(d1)
    pathlib.Path(blob_path(settings, sha1)).unlink()
    main_mod.db.close()
    fresh = Database(settings.db_path)  # 「重启」后的新连接
    try:
        reconcile(settings, fresh)
        view = cat.get_catalog(fresh, settings, STATION, DAY)
        assert view["state"] == cat.INVALID
        assert view["adopted_receipt_id"] == "CAT-K1"

        # 再开一个连接（第二次重启），结论必须稳定
        fresh2 = Database(settings.db_path)
        try:
            view2 = cat.get_catalog(fresh2, settings, STATION, DAY)
            assert view2["state"] == cat.INVALID
            candidates = cat.list_candidates(fresh2, STATION, DAY)
            assert [c["receipt_id"] for c in candidates] == ["CAT-K2"]
        finally:
            fresh2.close()
    finally:
        fresh.close()
        # 还原夹具持有的连接对象，避免 teardown 操作已关闭连接
        main_mod.db = Database(settings.db_path)


def test_concurrent_adoptions_only_one_wins_and_choice_never_corrupts(
    client, tmp_path, helpers
):
    """双实例并发采纳：同版本竞争，一个成功，另一个 409，选择保持一致。"""
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=2.0)
    _publish(client, "CAT-C1", d1)
    _publish(client, "CAT-C2", d2)
    version = client.get(CANDIDATES).json()["version"]

    outcomes: list[tuple[str, int]] = []
    lock = threading.Lock()

    def worker(rid: str):
        r = _adopt(client, rid, version)  # 两边都用同一过期竞争版本
        with lock:
            outcomes.append((rid, r.status_code))

    threads = [
        threading.Thread(target=worker, args=("CAT-C1",)),
        threading.Thread(target=worker, args=("CAT-C2",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(code for _, code in outcomes)
    assert statuses == [200, 409]
    final = client.get(CATALOG).json()
    assert final["state"] == "ACTIVE"
    winner = next(rid for rid, code in outcomes if code == 200)
    assert final["adopted_receipt_id"] == winner


def test_publishing_new_candidate_bumps_version_but_keeps_active_choice(
    client, tmp_path, helpers
):
    d1 = make_parquet(tmp_path / "a.parquet", station_id=STATION)
    _publish(client, "CAT-N1", d1)
    _adopt(client, "CAT-N1", client.get(CANDIDATES).json()["version"])
    chosen_version = client.get(CATALOG).json()["version"]

    d2 = make_parquet(tmp_path / "b.parquet", station_id=STATION, salt=5.0)
    _publish(client, "CAT-N2", d2)

    out = client.get(CATALOG).json()
    assert out["state"] == "ACTIVE"
    assert out["adopted_receipt_id"] == "CAT-N1"  # 选择不被新候选改变
    assert out["version"] == chosen_version + 1
    # 旧版本改选被拒
    r = _adopt(client, "CAT-N2", chosen_version)
    assert r.status_code == 409


def test_bad_dates_and_adopt_payload_validation(client):
    # 形状不合法 → FastAPI 路径正则 422
    r = client.get("/catalogs/stations/BUOY-CAT/days/20261115/candidates")
    assert r.status_code == 422
    # 形状合法但不是真实日历日期 → 领域校验 422
    r = client.get("/catalogs/stations/BUOY-CAT/days/2026-13-40")
    assert r.status_code == 422
    # 采纳体缺字段 / 版本为负 → 422
    r = client.post(ADOPTIONS, json={"receipt_id": "X"})
    assert r.status_code == 422
    r = client.post(ADOPTIONS, json={"receipt_id": "X", "expected_version": -1})
    assert r.status_code == 422
    # 从未有任何接收的站日也能查到 VACANT 目录
    r = client.get("/catalogs/stations/UNKNOWN/days/2026-11-15")
    assert r.status_code == 200
    assert r.json()["state"] == "VACANT"
