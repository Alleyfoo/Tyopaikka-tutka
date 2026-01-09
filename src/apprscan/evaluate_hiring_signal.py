"""Evaluate hiring-signal heuristics against stored fixtures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .hiring_scan import evaluate_html


def _eval_set(items):
    total = 0
    correct = 0
    tp = fp = fn = 0
    uncertain = 0
    for item in items:
        html = item["html"]
        expected = item["label"]
        url = item["url"]
        result = evaluate_html(html, url=url)
        predicted = str(result.get("signal") or "").lower()
        total += 1
        if predicted == expected:
            correct += 1
        if predicted == "yes" and expected == "yes":
            tp += 1
        elif predicted == "yes" and expected != "yes":
            fp += 1
        elif predicted != "yes" and expected == "yes":
            fn += 1
        if predicted == "unclear":
            uncertain += 1
    accuracy = correct / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    uncertain_rate = uncertain / total if total else 0.0
    return {
        "total": total,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "uncertain_rate": uncertain_rate,
    }


def _load_fixture_items(fixtures_dir: Path):
    labels_path = fixtures_dir / "labels.json"
    if not labels_path.exists():
        raise FileNotFoundError(f"Missing labels: {labels_path}")
    labels = json.loads(labels_path.read_text(encoding="utf-8"))
    items = []
    for item in labels:
        file_name = item.get("file")
        expected = str(item.get("label") or "").lower()
        if not file_name or not expected:
            continue
        html_path = fixtures_dir / file_name
        html = html_path.read_text(encoding="utf-8")
        items.append(
            {
                "html": html,
                "label": expected,
                "url": f"https://example.com/{file_name}",
            }
        )
    return items


def _check_thresholds(metrics, label, args) -> bool:
    failed = False
    if args.min_precision is not None and metrics["precision"] < args.min_precision:
        print(f"{label}: precision below threshold: {metrics['precision']:.2f} < {args.min_precision:.2f}")
        failed = True
    if args.min_recall is not None and metrics["recall"] < args.min_recall:
        print(f"{label}: recall below threshold: {metrics['recall']:.2f} < {args.min_recall:.2f}")
        failed = True
    if args.max_uncertain is not None and metrics["uncertain_rate"] > args.max_uncertain:
        print(f"{label}: uncertain rate above threshold: {metrics['uncertain_rate']:.2f} > {args.max_uncertain:.2f}")
        failed = True
    return failed


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate hiring-signal heuristics.")
    parser.add_argument(
        "--fixtures",
        default="tests/fixtures/hiring_signal",
        help="Path to fixtures directory.",
    )
    parser.add_argument("--min-precision", type=float, default=None, help="Minimum yes precision.")
    parser.add_argument("--min-recall", type=float, default=None, help="Minimum yes recall.")
    parser.add_argument("--max-uncertain", type=float, default=None, help="Maximum uncertain rate.")
    args = parser.parse_args()

    fixtures_dir = Path(args.fixtures)
    try:
        items = _load_fixture_items(fixtures_dir)
    except FileNotFoundError as exc:
        print(str(exc))
        return 2

    base_metrics = _eval_set(items)
    print("Fixture metrics")
    print(f"Total: {base_metrics['total']}")
    print(f"Accuracy: {base_metrics['accuracy']:.2f}")
    print(f"Yes precision: {base_metrics['precision']:.2f}")
    print(f"Yes recall: {base_metrics['recall']:.2f}")
    print(f"Uncertain rate: {base_metrics['uncertain_rate']:.2f}")

    failed = _check_thresholds(base_metrics, "fixtures", args)

    golden_path = fixtures_dir / "golden.jsonl"
    if golden_path.exists():
        golden_items = []
        for line in golden_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            text = str(payload.get("text") or "")
            url = str(payload.get("url") or "https://example.com")
            label = str(payload.get("label") or "").lower()
            html = f"<html><body>{text}</body></html>"
            if label:
                golden_items.append({"html": html, "label": label, "url": url})
        if golden_items:
            golden_metrics = _eval_set(golden_items)
            print("Golden metrics")
            print(f"Total: {golden_metrics['total']}")
            print(f"Accuracy: {golden_metrics['accuracy']:.2f}")
            print(f"Yes precision: {golden_metrics['precision']:.2f}")
            print(f"Yes recall: {golden_metrics['recall']:.2f}")
            print(f"Uncertain rate: {golden_metrics['uncertain_rate']:.2f}")
            failed = _check_thresholds(golden_metrics, "golden", args) or failed

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
