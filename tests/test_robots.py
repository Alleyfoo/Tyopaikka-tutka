from urllib.robotparser import RobotFileParser

from apprscan.jobs.robots import RobotsChecker


def test_robots_allows_when_missing():
    rc = RobotsChecker()
    parser = RobotFileParser()
    parser.parse([])
    rc.cache["example.com"] = parser
    assert rc.can_fetch("https://example.com/jobs")


def test_robots_can_fetch_detail():
    rc = RobotsChecker()
    parser = RobotFileParser()
    parser.parse([])
    rc.cache["example.com"] = parser
    allowed, rule = rc.can_fetch_detail("https://example.com/jobs")
    assert allowed is True
    assert rule is None
