from pathlib import Path

from apprscan.geocode import geocode_address, get_cached, set_cached


def test_geocode_uses_cache(tmp_path, mocker):
    cache = tmp_path / "geo.sqlite"
    set_cached("Testikatu 1, 00100, Helsinki", 1.0, 2.0, cache_path=cache)
    mock_geocoder = mocker.Mock()

    lat, lon, provider, cached = geocode_address(
        "Testikatu 1, 00100, Helsinki", cache_path=cache, geocoder=mock_geocoder
    )

    assert (lat, lon) == (1.0, 2.0)
    assert provider == "cache"
    assert cached is True
    mock_geocoder.assert_not_called()


def test_set_and_get_cache(tmp_path):
    cache = tmp_path / "geo.sqlite"
    set_cached("Addr", 10.0, 20.0, cache_path=cache)
    cached = get_cached("Addr", cache_path=cache)
    assert cached == (10.0, 20.0)
