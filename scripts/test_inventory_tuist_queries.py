"""Regression coverage for datasource attribution and extraction omissions."""

import subprocess
import tempfile
import unittest
from pathlib import Path

from inventory_tuist_queries import datasource_selections, extract, git


class InventoryTests(unittest.TestCase):
    def test_datasource_variable_overrides_incorrect_query_group(self):
        document = {"spec": {
            "variables": [{"kind": "DatasourceVariable", "spec": {
                "name": "logs", "pluginId": "loki",
                "current": {"text": "selected-logs", "value": "logs-id"},
            }}],
            "elements": {"hidden/panel": {"kind": "Panel", "spec": {
                "title": "Logs", "vizConfig": {"group": "logs"},
                "data": {"queries": [{"kind": "PanelQuery", "spec": {
                    "hidden": True, "query": {"kind": "DataQuery",
                        "group": "prometheus", "datasource": {"name": "$logs"},
                        "spec": {"expr": '{job="cache"} | json'},
                    },
                }}]},
            }}},
        }}
        records = extract(document)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["language"], "loki")
        self.assertEqual(records[0]["declared_group"], "prometheus")
        self.assertEqual(records[0]["language_source"], "datasource_variable")
        self.assertIn("hidden~1panel", records[0]["pointer"])
        self.assertEqual(datasource_selections(document), {
            "selected-logs": {"loki"}, "logs-id": {"loki"},
        })

    def test_nested_legacy_panels_and_literal_selection(self):
        document = {"spec": {"panels": [{
            "type": "row", "id": 1, "title": "Collapsed", "collapsed": True,
            "panels": [{"type": "timeseries", "id": 2, "title": "Rate",
                "datasource": "metrics-name", "targets": [
                    {"expr": "irate(requests_total[1m])", "hide": True},
                ],
            }],
        }], "templating": {"list": [{
            "type": "query", "name": "instance", "datasource": "metrics-name",
            "query": "label_values(requests_total, instance)",
        }]}}}
        records = extract(document, {"metrics-name": "prometheus"})
        self.assertEqual([r["language"] for r in records], ["prometheus", "prometheus"])
        self.assertEqual([r["purpose"] for r in records], ["panel", "variable"])
        self.assertTrue(records[0]["request"]["hide"])
        self.assertEqual(records[0]["language_source"], "repository_datasource_selection")
        self.assertEqual(records[1]["language_source"], "repository_datasource_selection")
        self.assertEqual(records[0]["pointer"], "/spec/panels/0/panels/0/targets/0")

    def test_unresolved_literal_is_not_guessed_from_its_name(self):
        records = extract({"panels": [{"datasource": "looks-like-prometheus",
            "targets": [{"expr": "up"}],
        }]})
        self.assertEqual(records[0]["language"], "unknown")

    def test_unknown_query_container_fails_even_alongside_valid_request(self):
        with self.assertRaisesRegex(ValueError, "Unsupported query container"):
            extract({"panels": [{"targets": [{"expr": "up"}]},
                {"queries": [{"expr": "lost_request"}]}]})

    def test_unrecognized_query_panel_fails_but_text_panel_is_allowed(self):
        with self.assertRaisesRegex(ValueError, "no recognized requests"):
            extract({"panels": [{"type": "timeseries", "id": 1, "title": "New shape"}]})
        self.assertEqual(extract({"panels": [{"type": "text", "id": 1, "title": "Notes"}]}), [])

    def test_untitled_query_panel_is_checked_and_dashboard_list_is_allowed(self):
        with self.assertRaisesRegex(ValueError, "no recognized requests"):
            extract({"panels": [{"type": "timeseries", "id": 1}]})
        self.assertEqual(extract({"panels": [{"type": "dashlist", "id": 1}]}), [])

    def test_git_source_bytes_preserve_carriage_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(["git", "init", "--quiet", directory], check=True)
            original = b'{"panels": []}\r\n'
            blob = subprocess.check_output(
                ["git", "-C", directory, "hash-object", "-w", "--stdin"], input=original
            ).decode().strip()
            self.assertEqual(git(Path(directory), "show", blob, text=False), original)


if __name__ == "__main__":
    unittest.main()
