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
