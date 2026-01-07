from apprscan.filters import exclude_company, industry_pass


def test_exclude_company_by_form_and_name():
    row_form = {"companyForm": "Asunto-osakeyhtiö", "name": "Test"}
    excl, reason = exclude_company(row_form)
    assert excl and "company_form" in reason

    row_name = {"name": "Kiinteistö Oy Testi"}
    excl, reason = exclude_company(row_name)
    assert excl and "name_match" in reason

    row_ok = {"companyForm": "OY", "name": "Hyvä Oy"}
    excl, reason = exclude_company(row_ok)
    assert excl is False and reason is None


def test_industry_pass_whitelist_and_blacklist():
    wl = ["koulutus"]
    bl = ["holding"]

    passed, reason, hard = industry_pass({"mainBusinessLine": "Koulutuspalvelut"}, wl, bl)
    assert passed and reason == "whitelist:koulutus"
    assert hard is False

    passed, reason, hard = industry_pass({"mainBusinessLine": "Holding-yhtiö"}, wl, bl)
    assert passed is False and reason == "blacklist:holding" and hard is True

    passed, reason, hard = industry_pass({"mainBusinessLine": "Metalli"}, wl, bl)
    assert passed is False and reason == "not_in_whitelist" and hard is False
