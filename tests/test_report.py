import json

import pandas as pd

from apprscan.report import export_reports


def test_export_reports_creates_files(tmp_path):
    df = pd.DataFrame(
        {
            "name": ["Test Co"],
            "lat": [60.0],
            "lon": [24.0],
            "nearest_station": ["Asema"],
            "distance_km": [0.5],
        }
    )
    out_dir = tmp_path / "out"
    export_reports(df, out_dir)

    excel = out_dir / "companies.xlsx"
    geojson = out_dir / "companies.geojson"
    html = out_dir / "companies_map.html"

    assert excel.exists()
    assert html.exists()
    assert geojson.exists()

    data = json.loads(geojson.read_text(encoding="utf-8"))
    assert data["features"][0]["properties"]["name"] == "Test Co"
