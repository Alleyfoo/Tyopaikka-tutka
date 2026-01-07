import responses
from responses import matchers

from apprscan.prh_client import PRH_BASE, fetch_companies


def _url():
    return f"{PRH_BASE}/companies"


@responses.activate
def test_pagination_stops_on_empty():
    responses.add(
        responses.GET,
        _url(),
        json={"companies": [{"id": 1}], "totalResults": 150},
        match=[matchers.query_param_matcher({"location": "Helsinki", "page": "0"})],
    )
    responses.add(
        responses.GET,
        _url(),
        json={"companies": [{"id": 2}], "totalResults": 150},
        match=[matchers.query_param_matcher({"location": "Helsinki", "page": "1"})],
    )
    responses.add(
        responses.GET,
        _url(),
        json={"companies": []},
        match=[matchers.query_param_matcher({"location": "Helsinki", "page": "2"})],
    )

    rows = fetch_companies("Helsinki")
    assert [r["id"] for r in rows] == [1, 2]


@responses.activate
def test_results_key_is_supported():
    responses.add(
        responses.GET,
        _url(),
        json={"results": [{"id": 10}]},
        match=[matchers.query_param_matcher({"location": "Espoo", "page": "0"})],
    )
    responses.add(
        responses.GET,
        _url(),
        json={"results": []},
        match=[matchers.query_param_matcher({"location": "Espoo", "page": "1"})],
    )

    rows = fetch_companies("Espoo")
    assert [r["id"] for r in rows] == [10]


@responses.activate
def test_max_pages_limits_requests():
    responses.add(
        responses.GET,
        _url(),
        json={"companies": [{"id": 1}], "totalResults": 500},
        match=[matchers.query_param_matcher({"location": "Vantaa", "page": "0"})],
    )
    responses.add(
        responses.GET,
        _url(),
        json={"companies": [{"id": 2}], "totalResults": 500},
        match=[matchers.query_param_matcher({"location": "Vantaa", "page": "1"})],
    )

    rows = fetch_companies("Vantaa", max_pages=1)
    assert [r["id"] for r in rows] == [1]
    # Second response should not be used (still queued).
    assert len(responses.calls) == 1
