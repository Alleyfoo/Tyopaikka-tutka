import pandas as pd

from apprscan.normalize import clean_address, normalize_companies, pick_first


def test_pick_first_and_clean_address():
    row = pd.Series({"addresses.0.street": "Katu 1", "postCode": "", "_source_city": "Espoo"})
    street = pick_first(row, ["addresses.0.street", "street"])
    city = pick_first(row, ["city", "_source_city"])
    assert street == "Katu 1"
    assert city == "Espoo"
    assert clean_address(street, "00100", city) == "Katu 1, 00100, Espoo"


def test_normalize_companies_builds_full_address():
    rows = [
        {"addresses": [{"street": "Katu 1", "postCode": "00100", "city": "Helsinki"}]},
        {"street": "Fallback 2", "postCode": "00200", "_source_city": "Espoo"},
        {"postCode": "00300", "domicile": "Vantaa"},
    ]
    df = normalize_companies(rows)
    assert list(df["full_address"]) == [
        "Katu 1, 00100, Helsinki",
        "Fallback 2, 00200, Espoo",
        "00300, Vantaa",
    ]
    # industry present even if groups missing
    assert "industry" in df.columns


def test_normalize_companies_picks_active_official_name():
    rows = [
        {
            "names": [
                {"name": "Vanha Oy", "type": "1", "registrationDate": "2020-01-01", "endDate": "2023-01-01"},
                {"name": "Uusi Oy", "type": "1", "registrationDate": "2024-01-01"},
                {"name": "Muu Nimi", "type": "2", "registrationDate": "2025-01-01"},
            ],
            "addresses": [{"street": "Katu", "postCode": "00100", "city": "Helsinki"}],
        }
    ]
    df = normalize_companies(rows)
    assert df.loc[0, "name"] == "Uusi Oy"
