"""校验器边界测试：任意 JSON 值都返回结构化问题而不是崩溃。"""

import json
import unittest

from src.event_consumer_guard import EVENT_KINDS, Problem, validate_event


def make_event(**overrides):
    record = {
        "event_id": "evt-1",
        "kind": "EVENT_PUBLISHED",
        "occurred_at": "2026-09-20T09:00:00+08:00",
        "subject_id": "sub-1",
        "payload": {"note": "ok"},
    }
    record.update(overrides)
    return record


class GarbageInputTest(unittest.TestCase):
    def test_non_object_inputs_return_problems_instead_of_raising(self):
        # 值班事故中的三种混入值：null、数组、字符串（外加数字/布尔）。
        for garbage in (None, [], ["a", 1], "just-a-string", 42, 3.14, True):
            with self.subTest(value=garbage):
                problems = validate_event(garbage)
                self.assertTrue(problems)
                self.assertEqual(problems[0].field, "<record>")
                self.assertEqual(problems[0].code, "bad_shape")
                self.assertIsInstance(problems[0].message, str)

    def test_empty_object_locates_every_missing_field(self):
        problems = validate_event({})
        codes = {(p.field, p.code) for p in problems}
        for name in ("event_id", "kind", "occurred_at", "subject_id", "payload"):
            self.assertIn((name, "missing_field"), codes)

    def test_problem_is_structured_and_json_serializable(self):
        problems = validate_event(None)
        dumped = json.dumps([p.as_dict() for p in problems], ensure_ascii=False)
        self.assertEqual(json.loads(dumped)[0]["code"], "bad_shape")
        self.assertIsInstance(problems[0], Problem)

    def test_unknown_kind_is_localized_separately(self):
        problems = validate_event(make_event(kind="SOMETHING_ELSE"))
        self.assertEqual(
            [(p.field, p.code) for p in problems], [("kind", "unknown_kind")]
        )

    def test_non_string_kind_does_not_raise(self):
        for bad_kind in (None, 12, ["EVENT_PUBLISHED"], {"k": 1}):
            with self.subTest(bad_kind=bad_kind):
                problems = validate_event(make_event(kind=bad_kind))
                self.assertIn(("kind", "unknown_kind"), {(p.field, p.code) for p in problems})

    def test_bad_timestamps_are_localized(self):
        for bad_ts in ("2026-09-20", "yesterday", None, 1726794000, "09:00:00"):
            with self.subTest(bad_ts=bad_ts):
                problems = validate_event(make_event(occurred_at=bad_ts))
                self.assertIn(
                    ("occurred_at", "bad_timestamp"),
                    {(p.field, p.code) for p in problems},
                )

    def test_z_suffix_timestamp_is_accepted(self):
        self.assertEqual(
            validate_event(make_event(occurred_at="2026-09-20T01:00:00Z")), []
        )

    def test_bad_subject_and_event_ids_are_localized(self):
        problems = validate_event(
            make_event(event_id="", subject_id=None)
        )
        fields = {p.field for p in problems}
        self.assertIn("subject_id", fields)
        self.assertIn("event_id", fields)
        self.assertTrue(all(p.code == "bad_subject" for p in problems))

    def test_payload_shape_is_localized(self):
        problems = validate_event(
            make_event(
                kind="BENEFIT_REDEEMED",
                payload={"benefit_id": "b-1"},  # 缺少 quota
            )
        )
        self.assertEqual([(p.field, p.code) for p in problems], [("payload", "bad_payload")])

        problems = validate_event(
            make_event(kind="COMPLAINT_OPENED", payload="not-an-object")
        )
        self.assertEqual([(p.field, p.code) for p in problems], [("payload", "bad_payload")])

    def test_multiple_problems_are_all_reported(self):
        problems = validate_event(
            {
                "event_id": "evt-1",
                "kind": "NO_SUCH_KIND",
                "occurred_at": "bad-time",
                # subject_id 与 payload 整体缺失
            }
        )
        self.assertEqual(
            sorted(p.code for p in problems),
            ["bad_timestamp", "missing_field", "missing_field", "unknown_kind"],
        )

    def test_every_known_kind_accepts_a_minimal_event(self):
        payloads = {
            "BENEFIT_REDEEMED": {"benefit_id": "b-1", "quota": 1},
            "COMPLAINT_OPENED": {"complaint_id": "c-1", "reason": "r"},
            "REMEDY_SETTLED": {"remedy_id": "r-1", "amount": 10},
        }
        for kind in EVENT_KINDS:
            with self.subTest(kind=kind):
                self.assertEqual(
                    validate_event(make_event(kind=kind, payload=payloads.get(kind, {"note": "ok"}))),
                    [],
                )


if __name__ == "__main__":
    unittest.main()
