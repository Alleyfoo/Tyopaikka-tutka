import time

from requests import Response

from apprscan.jobs.fetch import fetch_url


class DummyClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class DummySession:
    def __init__(self):
        self.calls = 0

    def get(self, url, timeout=20, headers=None, allow_redirects=True):
        self.calls += 1
        resp = Response()
        resp.status_code = 200
        resp.url = url
        resp._content = b"<html></html>"
        return resp


def test_rate_limit_respected(monkeypatch):
    clock = DummyClock()
    monkeypatch.setattr(time, "time", clock.time)
    monkeypatch.setattr(time, "sleep", clock.sleep)

    session = DummySession()
    state = {}
    # First call sets timestamp, no sleep
    res, reason = fetch_url(session, "https://example.com", rate_limit_state=state, req_per_second_per_domain=2.0)
    assert res is not None
    initial_sleeps = list(clock.sleeps)
    # Second call should wait because time hasn't advanced
    res2, reason2 = fetch_url(session, "https://example.com", rate_limit_state=state, req_per_second_per_domain=2.0)
    assert res2 is not None
    assert len(clock.sleeps) >= len(initial_sleeps) + 1
    assert any(s > 0 for s in clock.sleeps)
