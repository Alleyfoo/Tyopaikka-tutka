import pandas as pd

from apprscan.cli import main


def test_cli_run_with_skip_geocode(monkeypatch, tmp_path):
    fake_rows = [
        {"addresses": [{"street": "Testikatu 1", "postCode": "00100", "city": "Helsinki"}], "name": "Test"},
    ]

    monkeypatch.setattr("apprscan.cli.fetch_companies", lambda **kwargs: fake_rows)
    monkeypatch.setattr(
        "apprscan.cli.load_stations",
        lambda use_local=True, path=None: pd.DataFrame(
            {"station_name": ["Asema"], "lat": [60.0], "lon": [24.0]}
        ),
    )

    out_dir = tmp_path / "out"
    code = main(
        [
            "run",
            "--cities",
            "Helsinki",
            "--skip-geocode",
            "--stations-file",
            str(tmp_path / "stations_fi.csv"),
            "--out",
            str(out_dir),
        ]
    )

    assert code == 0
