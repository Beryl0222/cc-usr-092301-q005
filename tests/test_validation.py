"""校验器输入边界测试：任意 JSON 值都返回结构化问题而不是崩溃。"""

import json
import unittest

from src.event_consumer_guard import (
    EVENT_KINDS,
    validate_event,
    validate_event_detailed,
)


def _good_event(**overrides):
    record = {
        "event_id": "e-1",
        "kind": "BENEFIT_REDEEMED",
        "occurred_at": "2026-09-20T09:00:00+08:00",
        "subject_id": "subject-1",
        "payload": {"benefit_id": "b-1", "redeem_amount": 10},
    }
    record.update(overrides)
    return record


class ArbitraryJsonValueTest(unittest.TestCase):
    def test_null_array_string_number_do_not_raise(self):
        for raw in (None, [], [1, 2], "oops", 42, 3.14, True, False, {}):
            with self.subTest(raw=raw):
                report = validate_event_detailed(raw)
                self.assertFalse(report.valid)
                codes = report.problem_codes()
                if isinstance(raw, dict):
                    # 空对象：逐字段报缺失，而不是整体崩溃
                    self.assertIn("missing_field", codes)
                else:
                    self.assertEqual(codes, ["not_an_object"])
                    self.assertIsNone(report.problems[0].field)

    def test_legacy_validate_event_never_raises(self):
        for raw in (None, ["x"], "x", 1, True):
            with self.subTest(raw=raw):
                problems = validate_event(raw)
                self.assertIsInstance(problems, list)
                self.assertTrue(problems)
        # 空对象基线行为：五个必填字段名
        self.assertEqual(
            validate_event({}),
            ["event_id", "kind", "occurred_at", "subject_id", "payload"],
        )

    def test_report_is_serializable(self):
        report = validate_event_detailed(None)
        json.dumps(report.to_dict(), ensure_ascii=False)


class FieldLocationTest(unittest.TestCase):
    def _codes_for(self, **overrides):
        return validate_event_detailed(_good_event(**overrides)).problem_codes()

    def test_each_missing_field_is_located_separately(self):
        report = validate_event_detailed(
            {"kind": "BENEFIT_REDEEMED", "payload": {}}
        )
        fields = {p.field for p in report.problems if p.code == "missing_field"}
        self.assertEqual(fields, {"event_id", "occurred_at", "subject_id"})

    def test_unknown_kind(self):
        report = validate_event_detailed(_good_event(kind="CHARGEBACK_FROZEN"))
        kinds = [p for p in report.problems if p.field == "kind"]
        self.assertEqual(len(kinds), 1)
        self.assertEqual(kinds[0].code, "unknown_kind")

    def test_non_string_kind(self):
        report = validate_event_detailed(_good_event(kind=123))
        self.assertIn(
            ("kind", "invalid_type"),
            [(p.field, p.code) for p in report.problems],
        )

    def test_bad_timestamps_are_located(self):
        for ts in ("2026/09/20 09:00", "not-a-time", 1726798800, None):
            with self.subTest(ts=ts):
                fields = [
                    (p.field, p.code)
                    for p in validate_event_detailed(_good_event(occurred_at=ts)).problems
                ]
                self.assertIn(("occurred_at", "invalid_timestamp"), fields)

    def test_accepted_timestamp_shapes(self):
        for ts in (
            "2026-09-20T09:00:00+08:00",
            "2026-09-20T01:00:00Z",
            "2026-09-20 09:00:00",
        ):
            with self.subTest(ts=ts):
                self.assertTrue(validate_event_detailed(_good_event(occurred_at=ts)).valid)

    def test_subject_identifier_shape(self):
        for bad in ("", "   ", 9, None, ["s"]):
            with self.subTest(bad=bad):
                codes = self._codes_for(subject_id=bad)
                self.assertIn("invalid_subject", codes)
        for bad in ("", 7, None):
            with self.subTest(bad=bad):
                codes = self._codes_for(event_id=bad)
                self.assertIn("invalid_identifier", codes)

    def test_payload_must_be_object(self):
        fields = [
            (p.field, p.code)
            for p in validate_event_detailed(_good_event(payload=["nope"])).problems
        ]
        self.assertIn(("payload", "invalid_payload_shape"), fields)

    def test_payload_shape_per_kind(self):
        report = validate_event_detailed(
            _good_event(payload={"benefit_id": "b-1"})
        )
        self.assertIn(
            ("payload.redeem_amount", "missing_payload_field"),
            [(p.field, p.code) for p in report.problems],
        )
        # 未知 kind 时不套用载荷 schema，只报 kind
        report = validate_event_detailed(_good_event(kind="WHAT", payload={}))
        self.assertEqual(
            [p.code for p in report.problems if p.field and p.field.startswith("payload.")],
            [],
        )

    def test_valid_event_and_known_kinds(self):
        self.assertTrue(validate_event_detailed(_good_event()).valid)
        for kind in EVENT_KINDS:
            self.assertIsInstance(kind, str)


if __name__ == "__main__":
    unittest.main()
