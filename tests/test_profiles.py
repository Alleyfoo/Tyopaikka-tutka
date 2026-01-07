from pathlib import Path

from apprscan.profiles import load_profiles, apply_profile


def test_load_and_apply_profile(tmp_path: Path):
    cfg = tmp_path / "profiles.yaml"
    cfg.write_text(
        """
demo:
  include_tags: "data"
  min_score: 5
        """,
        encoding="utf-8",
    )
    profiles = load_profiles(cfg)
    assert "demo" in profiles
    merged = apply_profile("demo", profiles, {"include_tags": ""})
    assert merged["include_tags"] == "data"
    # CLI override wins
    merged2 = apply_profile("demo", profiles, {"include_tags": "it_support"})
    assert merged2["include_tags"] == "it_support"
