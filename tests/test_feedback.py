import pandas as pd

from apprscan.feedback import LABELS, connect, load, load_log, record


def _db(tmp_path):
    return connect(tmp_path / "feedback.sqlite")


def test_record_and_load_roundtrip(tmp_path):
    conn = _db(tmp_path)
    record(conn, "0100729-8", "kiinnostaa")
    frame = load(conn)
    assert len(frame) == 1
    assert frame["business_id"].iloc[0] == "0100729-8"
    assert frame["label"].iloc[0] == "kiinnostaa"
    assert frame["source"].iloc[0] == "dashboard"


def test_overwrite_keeps_one_row_but_logs_history(tmp_path):
    conn = _db(tmp_path)
    record(conn, "1", "kiinnostaa")
    record(conn, "1", "ei_kiinnosta")
    assert len(load(conn)) == 1
    assert load(conn)["label"].iloc[0] == "ei_kiinnosta"
    log = load_log(conn)
    assert len(log) == 2
    assert list(log["label"]) == ["kiinnostaa", "ei_kiinnosta"]


def test_invalid_label_raises(tmp_path):
    conn = _db(tmp_path)
    try:
        record(conn, "1", "maybe")
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "maybe" in str(exc)
    assert len(load(conn)) == 0


def test_all_labels_accepted(tmp_path):
    conn = _db(tmp_path)
    for i, label in enumerate(LABELS):
        record(conn, f"bid-{i}", label)
    assert sorted(load(conn)["label"]) == sorted(LABELS)