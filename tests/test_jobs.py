import pandas as pd

from apprscan.jobs import detect_tags, parse_jsonld_jobs, load_domain_mapping


def test_detect_tags():
    tags = detect_tags("Oppisopimus-kehittäjä", "Oppisopimus")
    assert "oppisopimus" in tags
    assert "junior" not in tags


def test_parse_jsonld_jobs():
    html = """
    <html><head>
    <script type="application/ld+json">
    {
      "@type": "JobPosting",
      "title": "Junior Data Engineer",
      "url": "/jobs/1",
      "description": "Data engineer oppisopimus",
      "jobLocation": {"address": {"addressLocality": "Helsinki"}},
      "employmentType": "full-time",
      "datePosted": "2024-01-01"
    }
    </script>
    </head><body></body></html>
    """
    company = {"business_id": "123", "name": "Testi Oy", "domain": "example.com"}
    jobs = parse_jsonld_jobs(html, "https://example.com/careers", company, "2024-01-02T00:00:00Z")
    assert len(jobs) == 1
    job = jobs[0]
    assert job.job_url == "https://example.com/jobs/1"
    assert "data" in job.tags or "oppisopimus" in job.tags
    assert job.location_text == "Helsinki"


def test_load_domain_mapping(tmp_path):
    csv_path = tmp_path / "domains.csv"
    pd.DataFrame({"business_id": ["123"], "domain": ["example.com"]}).to_csv(csv_path, index=False)
    mapping = load_domain_mapping(str(csv_path))
    assert mapping["123"] == "example.com"
