#!/usr/bin/env python3
"""Validate the planning snapshot and render its per-UID checklist.

This checks inventory integrity, not Pulso importer or evaluator conformance.
Run: python3 plans/alerting/validate_inventory.py [--write-matrix]
"""

import argparse
import collections
import copy
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EXPECTED_SOURCES = {
    "grafanacloud-prom": "prometheus",
    "grafanacloud-logs": "loki",
    "grafanacloud-usage": "prometheus",
    "dexgs9hv7rjswd": "clickhouse",
    "__expr__": "expression",
}
REFERENCE = re.compile(r"\$(?:\{([a-zA-Z_][a-zA-Z_0-9]*)\}|([a-zA-Z_][a-zA-Z_0-9]*))")


def dependencies(query):
    model = query["model"]
    if query["source_type"] != "expression":
        return []
    expression = model["expression"]
    if model["type"] == "math":
        return [a or b for a, b in REFERENCE.findall(expression)]
    assert model["type"] in {"threshold", "reduce"}, model["type"]
    return [expression]


def validate(rules):
    assert len(rules) == 115, "Snapshot must account for every inspected entry"
    assert len({rule["uid"] for rule in rules}) == 115, "Duplicate rule UID"
    assert sum(rule["record"] is not None for rule in rules) == 4
    for rule in rules:
        assert rule["evaluation_interval_seconds"] in {60, 300, 600}
        assert isinstance(rule["isPaused"], bool), rule["uid"]
        assert "keep_firing_for" in rule, rule["uid"]
        assert "keepFiringFor" not in rule, "Do not replace Grafana's actual keep_firing_for key"
        assert re.fullmatch(r"(?:\d+(?:ms|s|m|h|d|w|y))+", rule["keep_firing_for"]), rule["uid"]
        queries = {query["refId"]: query for query in rule["data"]}
        assert len(queries) == len(rule["data"]), rule["uid"]
        if rule["record"]:
            assert rule["record"]["from"] in queries, rule["uid"]
            assert rule["record"]["metric"] == rule["title"]
        else:
            assert rule["condition"] in queries, rule["uid"]
        visiting, visited = set(), set()

        def visit(ref):
            assert ref in queries, (rule["uid"], "missing reference", ref)
            assert ref not in visiting, (rule["uid"], "cycle", ref)
            if ref in visited:
                return
            visiting.add(ref)
            query = queries[ref]
            assert EXPECTED_SOURCES[query["datasourceUid"]] == query["source_type"]
            for dependency in dependencies(query):
                visit(dependency)
            visiting.remove(ref)
            visited.add(ref)

        for ref in queries:
            visit(ref)
    return collections.Counter(query["source_type"] for rule in rules for query in rule["data"])


def validate_original_snapshot(snapshot):
    original = copy.deepcopy(snapshot["rules"])
    for rule in original:
        rule.pop("evaluation_interval_seconds")
        for query in rule["data"]:
            query.pop("source_type")
    encoded = json.dumps(original, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert hashlib.sha256(encoded).hexdigest() == snapshot["provenance"]["original_rules_sha256"], "Original fields were changed or dropped"


def validate_notifications(rules):
    inventory = json.loads((ROOT / "notification-inventory.json").read_text())
    receivers = {item["receiver"] for item in inventory["integrations"]}
    assert inventory["notification_policy"]["receiver"] in receivers
    for route in inventory["notification_policy"]["routes"]:
        assert route["receiver"] in receivers
    for rule in rules:
        assert rule["folderUID"] in inventory["folder_paths"], rule["uid"]
        receiver = (rule.get("notification_settings") or {}).get("receiver")
        assert receiver is None or receiver in receivers, (rule["uid"], receiver)
    assert sum(rule["isPaused"] for rule in rules) == 2
    assert sum(rule["keep_firing_for"] != "0s" for rule in rules) == 13


def cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def render(rules):
    lines = [
        "# Grafana rule-by-rule modeling checklist",
        "",
        "Generated from `grafana-rule-inventory.json` by `validate_inventory.py --write-matrix`.",
        "All 115 UIDs are included. This is a planning inventory, not a claim of implemented compatibility.",
        "Every row still requires importer round-trip, source binding, numerical/label/error fixtures,",
        "routing fixtures and shadow comparison before its migration status can become approved.",
        "Source query definitions and annotation templates remain in the JSON snapshot.",
        "",
        "`main metrics` and `main logs` identify observed Grafana sources, NOT confirmed Pulso-resident inputs.",
        "Every selector producer/residency closure remains a migration blocker until verified; recording consumers must move with their inputs.",
        "`synthetic` identifies externally produced probe/threshold/log inputs and is not implicitly Alloy telemetry.",
        "`usage` requires Grafana's external usage metrics; `SQL` requires an existing external ClickHouse source.",
        "Mixed-source rules must remain mixed-source expression graphs. `policy` means notification-tree routing;",
        "`record` means no alert notification. Blank no-data/error settings belong to recording entries, not default alert policies.",
        "",
        "| UID | Rule | Observed sources (residency unverified) | Expression nodes | Cadence / for / keep-firing | Paused | No-data / error | Receiver |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    names = {"grafanacloud-prom": "main metrics", "grafanacloud-logs": "main logs", "grafanacloud-usage": "usage", "dexgs9hv7rjswd": "SQL"}
    for rule in sorted(rules, key=lambda r: (r["folderUID"], r["ruleGroup"], r["title"])):
        sources = sorted({names[q["datasourceUid"]] for q in rule["data"] if q["source_type"] != "expression"})
        if rule["folderUID"] == "grafana-synthetic-monitoring-app":
            sources = ["synthetic " + name for name in sources]
        exprs = sorted({q["model"]["type"] for q in rule["data"] if q["source_type"] == "expression"})
        if rule["record"]:
            receiver = "record"
        else:
            receiver = (rule["notification_settings"] or {}).get("receiver", "policy")
        row = [rule["uid"], rule["title"], ", ".join(sources), ", ".join(exprs) or "source condition", f"{rule['evaluation_interval_seconds']}s / {rule['for']} / {rule['keep_firing_for']}", "yes" if rule["isPaused"] else "no", f"{rule['noDataState'] or '-'} / {rule['execErrState'] or '-'}", receiver]
        lines.append("| " + " | ".join(cell(v) for v in row) + " |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-matrix", action="store_true")
    args = parser.parse_args()
    snapshot = json.loads((ROOT / "grafana-rule-inventory.json").read_text())
    assert snapshot["schema_version"] == 1
    validate_original_snapshot(snapshot)
    counts = validate(snapshot["rules"])
    validate_notifications(snapshot["rules"])
    matrix = render(snapshot["rules"])
    path = ROOT / "coverage-matrix.md"
    if args.write_matrix:
        path.write_text(matrix)
    else:
        assert path.read_text() == matrix, "Checklist is stale; regenerate with --write-matrix"
    print(f"Validated 115 unique entries, 111 alerts, four recordings, all references and acyclic expression graphs: {dict(counts)}")
    print("Original export digest, two paused rules, 13 nonzero keep-firing settings, folder paths and receiver references checked.")
    print("Inventory integrity only; importer/evaluator conformance remains unvalidated.")


if __name__ == "__main__":
    main()
