import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from run_local import Scenario, check_ports


class ScenarioChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.scenario = Scenario(
            SimpleNamespace(service_port=63124, admin_port=8091, timeout=1),
            Path(self.temporary.name),
        )

    def snapshot(self, rows):
        with patch("run_local.request", return_value={"success": True, "data": rows}):
            return self.scenario.snapshot([1, 2])

    def test_snapshot_matches_complete_rows_independent_of_order(self):
        self.assertIsNotNone(self.snapshot([{"Ordinal": 2}, {"Ordinal": 1}]))

    def test_snapshot_does_not_accept_missing_duplicate_or_extra_rows(self):
        for ordinals in ([], [1], [1, 1, 2], [1, 2, 3]):
            with self.subTest(ordinals=ordinals):
                self.assertIsNone(self.snapshot([{"Ordinal": value} for value in ordinals]))

    def test_unsuccessful_snapshot_is_not_empty_success(self):
        with patch("run_local.request", return_value={"success": False, "data": []}):
            with self.assertRaisesRegex(RuntimeError, "Snapshot request failed"):
                self.scenario.snapshot([1, 2])

    def test_delivery_collection_preserves_duplicates(self):
        folder = self.scenario.run_storage / "reactions" / "items"
        folder.mkdir(parents=True)
        records = [{"payload": {"request_body": {
            "result": {"type": "ADD", "after": {"Ordinal": ordinal}}
        }}} for ordinal in (1, 1, 2)]
        (folder / "outputs_00000.jsonl").write_text(
            "".join(json.dumps(record) + "\n" for record in records)
        )
        self.assertEqual(self.scenario.deliveries(), [1, 1, 2])

    def test_unexpected_deliveries_fail(self):
        with patch.object(self.scenario, "deliveries", return_value=[1, 2, 5]):
            with self.assertRaisesRegex(RuntimeError, "Unexpected deliveries"):
                self.scenario.wait_deliveries([1, 2])

    def test_duplicate_ports_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "Ports must be distinct"):
            check_ports([63124, 63124])


if __name__ == "__main__":
    unittest.main()