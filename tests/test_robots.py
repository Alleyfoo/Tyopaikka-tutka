from apprscan.jobs.robots import RobotsChecker


def test_robots_allows_when_missing():
    rc = RobotsChecker()
    # Unknown domain -> parser allow all
    assert rc.can_fetch("https://example.com/jobs")
