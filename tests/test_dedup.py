import pandas as pd

from apprscan.normalize import deduplicate_companies


def test_deduplicate_prefers_geocoded():
    df = pd.DataFrame(
        {
            "business_id": ["123", "123", "456"],
            "lat": [None, 60.0, 61.0],
            "lon": [None, 24.0, 25.0],
            "name": ["A", "B", "C"],
        }
    )
    out = deduplicate_companies(df)
    # Should keep geocoded row for 123 and both rows for 456 (only one).
    assert len(out) == 2
    assert set(out["name"]) == {"B", "C"}
