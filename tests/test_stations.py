import pandas as pd

from apprscan.stations import load_stations


def test_load_stations_prefers_local(tmp_path):
    csv_path = tmp_path / "stations_fi.csv"
    df_local = pd.DataFrame(
        {
            "station_name": ["Asema 1"],
            "lat": [60.0],
            "lon": [24.0],
            "country": ["FI"],
        }
    )
    df_local.to_csv(csv_path, index=False)

    df = load_stations(use_local=True, path=csv_path)

    assert len(df) == 1
    assert df.loc[0, "station_name"] == "Asema 1"
