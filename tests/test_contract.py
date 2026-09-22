import json
import unittest
from pathlib import Path

from src.event_consumer_guard import validate_event

class ContractTest(unittest.TestCase):
    def test_sample_matches_domain_contract(self):
        record = json.loads((Path(__file__).parents[1] / "data" / "sample.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_event(record), [])

if __name__ == "__main__":
    unittest.main()
