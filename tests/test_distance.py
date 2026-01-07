import pandas as pd

from apprscan.distance import haversine_km, nearest_station_from_df


def test_haversine_regression():
    # Helsinki (60.1699, 24.9384) to Tampere (61.4981, 23.7608) ~158 km
    dist = haversine_km(60.1699, 24.9384, 61.4981, 23.7608)
    assert 155 <= dist <= 161


def test_nearest_station_from_df():
    stations = pd.DataFrame(
        {"station_name": ["A", "B"], "lat": [60.0, 61.0], "lon": [24.0, 25.0]}
    )
    name, dist = nearest_station_from_df(60.05, 24.05, stations)
    assert name == "A"
    assert dist < 10
