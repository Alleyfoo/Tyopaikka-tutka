import pandas as pd

from apprscan.storage import load_employee_enrichment


def test_load_employee_enrichment(tmp_path):
    csv_path = tmp_path / "emp.csv"
    pd.DataFrame(
        {
            "businessId": ["123", "456"],
            "employee_count": [10, None],
            "employee_band": [None, "1-4"],
        }
    ).to_csv(csv_path, index=False)

    data = load_employee_enrichment(csv_path)
    assert "123" in data and data["123"]["employee_count"] == 10
    assert data["456"]["employee_band"] == "1-4"
