"""Tests for planning-inventory integrity, not alert evaluator conformance."""

import copy
import json
import unittest

import validate_inventory as inventory


class InventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = json.loads((inventory.ROOT / "grafana-rule-inventory.json").read_text())

    def test_complete_snapshot_and_notification_references(self):
        inventory.validate_original_snapshot(self.snapshot)
        counts = inventory.validate(self.snapshot["rules"])
        inventory.validate_notifications(self.snapshot["rules"])
        self.assertEqual(counts, {"prometheus": 105, "loki": 16, "clickhouse": 2, "expression": 107})

    def test_matrix_is_current(self):
        expected = inventory.render(self.snapshot["rules"])
        self.assertEqual((inventory.ROOT / "coverage-matrix.md").read_text(), expected)

    def test_original_field_loss_is_detected(self):
        snapshot = copy.deepcopy(self.snapshot)
        snapshot["rules"][0].pop("keep_firing_for")
        with self.assertRaisesRegex(AssertionError, "Original fields were changed or dropped"):
            inventory.validate_original_snapshot(snapshot)

    def test_wrong_keep_firing_key_is_detected(self):
        rules = copy.deepcopy(self.snapshot["rules"])
        rules[0]["keepFiringFor"] = rules[0].pop("keep_firing_for")
        with self.assertRaises(AssertionError):
            inventory.validate(rules)

    def test_missing_expression_reference_is_detected(self):
        rules = copy.deepcopy(self.snapshot["rules"])
        query = next(q for r in rules for q in r["data"] if q["model"].get("type") == "threshold")
        query["model"]["expression"] = "missing_ref"
        with self.assertRaises(AssertionError):
            inventory.validate(rules)

    def test_expression_cycle_is_detected(self):
        rules = copy.deepcopy(self.snapshot["rules"])
        query = next(q for r in rules for q in r["data"] if q["model"].get("type") == "threshold")
        query["model"]["expression"] = query["refId"]
        with self.assertRaises(AssertionError):
            inventory.validate(rules)

    def test_braced_cross_source_refs_are_preserved(self):
        rule = next(r for r in self.snapshot["rules"] if r["uid"] == "sm-failed-executions-5m-15b8b2b7")
        query = next(q for q in rule["data"] if q["refId"] == "condition")
        self.assertEqual(inventory.dependencies(query), ["executions", "threshold"])
        self.assertEqual({q["source_type"] for q in rule["data"]}, {"loki", "prometheus", "expression"})


if __name__ == "__main__":
    unittest.main()
