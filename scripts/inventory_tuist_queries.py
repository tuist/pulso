#!/usr/bin/env python3
"""Snapshot committed Tuist dashboard requests without reading credentials.

Only dashboard files from the selected Git revision are read. Output is stable
for the same revision, including queries in nested rows and hidden panels.
"""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


def git(repo, *args, text=True):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=text)


def pointer(parts):
    return "/" + "/".join(str(p).replace("~", "~0").replace("/", "~1") for p in parts)


def datasource_selections(document):
    """Repository evidence for literal datasource names, not a live registry."""
    body = document.get("spec", document)
    variables = body.get("variables", body.get("templating", {}).get("list", []))
    selections = {}
    for variable in variables:
        spec = variable.get("spec", variable)
        if variable.get("kind") == "DatasourceVariable" or spec.get("type") == "datasource":
            plugin = spec.get("pluginId", spec.get("query"))
            for field in ("text", "value"):
                values = spec.get("current", {}).get(field, [])
                for value in values if isinstance(values, list) else [values]:
                    if isinstance(value, str) and value and not value.startswith("$"):
                        selections.setdefault(value, set()).add(plugin)
    return selections


def extract(document, datasource_aliases=None):
    """Handle both Grafana dashboard schemas without expanding template values."""
    body = document.get("spec", document)
    variables = body.get("variables", body.get("templating", {}).get("list", []))
    datasource_variables = {}
    for variable in variables:
        spec = variable.get("spec", variable)
        plugin = spec.get("pluginId", spec.get("query"))
        if variable.get("kind") == "DatasourceVariable" or spec.get("type") == "datasource":
            datasource_variables[spec["name"]] = plugin

    def attribution(datasource):
        if isinstance(datasource, dict):
            if datasource.get("type"):
                return datasource["type"], "datasource_type"
            datasource = datasource.get("name", datasource.get("uid"))
        if isinstance(datasource, str):
            match = re.fullmatch(r"\$(?:\{(\w+)\}|(\w+))", datasource)
            if match:
                plugin = datasource_variables.get(match[1] or match[2])
                if plugin:
                    return plugin, "datasource_variable"
            if datasource in (datasource_aliases or {}):
                return datasource_aliases[datasource], "repository_datasource_selection"
        return "unknown", "unresolved"

    records = []

    def walk(node, parts=(), inherited=("unknown", "unresolved"), purpose="panel", title=None):
        if isinstance(node, list):
            for i, child in enumerate(node):
                walk(child, (*parts, i), inherited, purpose, title)
        elif isinstance(node, dict):
            if "datasource" in node:
                inherited = attribution(node["datasource"])
            if "title" in node:
                title = node["title"]
            if node.get("kind") == "DataQuery":
                spec = node.get("spec", {})
                plugin, evidence = attribution(node["datasource"]) if "datasource" in node else inherited
                if plugin == "unknown":
                    plugin, evidence = node.get("group", "unknown"), "query_group"
                records.append({
                    "pointer": pointer(parts), "purpose": purpose, "panel_title": title,
                    "language": plugin, "language_source": evidence,
                    "declared_group": node.get("group"), "request": spec,
                    "datasource": node.get("datasource"),
                })
                return
            if "queries" in node and not all(
                isinstance(query, dict) and query.get("kind") == "PanelQuery"
                for query in node["queries"]
            ):
                raise ValueError(f"Unsupported query container at {pointer(parts)}")
            if "target" in node:
                raise ValueError(f"Unsupported singular target at {pointer(parts)}")
            # Legacy queries may be strings or query objects. Preserve all fields
            # so discovery requests are not confused with expression evaluation.
            if "targets" in node:
                for i, target in enumerate(node["targets"]):
                    plugin, evidence = attribution(target["datasource"]) if "datasource" in target else inherited
                    records.append({
                        "pointer": pointer((*parts, "targets", i)), "purpose": purpose,
                        "panel_title": title,
                        "language": plugin, "language_source": evidence,
                        "declared_group": target.get("group"),
                        "request": target, "datasource": target.get("datasource", node.get("datasource")),
                    })
            if node.get("type") == "query" and "query" in node:
                records.append({
                    "pointer": pointer(parts), "purpose": "variable", "panel_title": None,
                    "language": inherited[0], "language_source": inherited[1],
                    "declared_group": node.get("group"),
                    "request": {"query": node["query"], "definition": node.get("definition")},
                    "datasource": node.get("datasource"),
                })
            for key, child in node.items():
                if key == "targets":
                    continue
                child_purpose = "variable" if key in ("variables", "templating") else "annotation" if key == "annotations" else purpose
                walk(child, (*parts, key), inherited, child_purpose, title)

    walk(document)
    # Text, row, and dashboard-list panels legitimately have no requests. Query-bearing panels
    # must have a supported shape; an unknown new shape cannot silently vanish.
    def check_panels(node, parts=()):
        if isinstance(node, list):
            for i, child in enumerate(node):
                check_panels(child, (*parts, i))
        elif isinstance(node, dict):
            panel = node.get("kind") == "Panel" or (
                len(parts) >= 2 and parts[-2] == "panels" and isinstance(parts[-1], int)
            )
            if panel:
                spec = node.get("spec", node)
                panel_type = spec.get("type", spec.get("vizConfig", {}).get("group"))
                if panel_type not in ("row", "text", "dashlist"):
                    prefix = pointer(parts) + "/"
                    if not any(record["pointer"].startswith(prefix) for record in records):
                        raise ValueError(f"Panel has no recognized requests at {pointer(parts)}")
            for key, child in node.items():
                check_panels(child, (*parts, key))

    check_panels(document)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", type=Path)
    parser.add_argument("--revision", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", action="store_true", help="Fail if output differs; never rewrite it")
    args = parser.parse_args()
    revision = git(args.repository, "rev-parse", "--verify", f"{args.revision}^{{commit}}").strip()
    paths = git(args.repository, "ls-tree", "-r", "--name-only", revision, "infra/grafana-dashboards").splitlines()
    documents = []
    aliases = {}
    for path in sorted(p for p in paths if p.endswith(".json")):
        content = git(args.repository, "show", f"{revision}:{path}", text=False)
        document = json.loads(content)
        if document.get("apiVersion") not in ("dashboard.grafana.app/v1", "dashboard.grafana.app/v2", None):
            raise ValueError(f"Unsupported dashboard schema in {path}")
        documents.append((path, content, document))
        for name, plugins in datasource_selections(document).items():
            aliases.setdefault(name, set()).update(plugins)
    # Conflicting literal names stay unresolved rather than choosing a plugin.
    aliases = {name: next(iter(plugins)) for name, plugins in aliases.items() if len(plugins) == 1}
    dashboards = []
    for path, content, document in documents:
        records = extract(document, aliases)
        if not records:
            raise ValueError(f"No requests extracted from {path}; check dashboard schema")
        body = document.get("spec", document)
        dashboards.append({
            "path": path, "source_sha256": hashlib.sha256(content).hexdigest(),
            "schema": document.get("apiVersion", "legacy"), "requests": records,
            "variables": body.get("variables", body.get("templating", {}).get("list", [])),
            "annotations": body.get("annotations", []),
        })
    if not dashboards:
        raise ValueError("No dashboards found at selected revision")
    output = {
        "format_version": 1, "repository": "tuist/tuist", "revision": revision,
        "scope": "Committed dashboard requests only; no live exports or payload fixtures",
        "compatibility_status": "unverified; see the workload inventory for feature-level gaps",
        "dashboards": dashboards,
    }
    serialized = json.dumps(output, indent=2, ensure_ascii=False) + "\n"
    if args.check:
        if args.output.read_text() != serialized:
            raise SystemExit(f"Snapshot differs: {args.output}")
    else:
        args.output.write_text(serialized)
    print(f"Captured {sum(len(d['requests']) for d in dashboards)} requests from {len(dashboards)} dashboards at {revision}")


if __name__ == "__main__":
    main()
