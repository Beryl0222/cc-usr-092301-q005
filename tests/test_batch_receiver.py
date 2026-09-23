"""可靠批量接收路径测试：隔离、顺序落账、幂等、冲突、崩溃恢复、重提。"""

import json
import tempfile
import unittest
from pathlib import Path

from src.batch_receiver import BatchReceiver


def event(event_id, kind="EVENT_PUBLISHED", **overrides):
    record = {
        "event_id": event_id,
        "kind": kind,
        "occurred_at": "2026-09-20T09:00:00+08:00",
        "subject_id": "sub-1",
        "payload": {"note": "ok"},
    }
    record.update(overrides)
    return record


def redeem(event_id, quota=1):
    return event(
        event_id,
        kind="BENEFIT_REDEEMED",
        payload={"benefit_id": "benefit-1", "quota": quota},
    )


class RecordingEffects:
    """记录副作用调用次数；可按 event_id 注入异常模拟进程中断。"""

    def __init__(self, fail_event_id=None):
        self.calls = []
        self.fail_event_id = fail_event_id

    def write_off_benefit(self, evt):
        self.calls.append(("write_off_benefit", evt["event_id"]))
        if self.fail_event_id == evt["event_id"]:
            raise RuntimeError("simulated crash during benefit write-off")

    def close_complaint(self, evt):
        self.calls.append(("close_complaint", evt["event_id"]))
        if self.fail_event_id == evt["event_id"]:
            raise RuntimeError("simulated crash during complaint close")


class ReceiverTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = str(Path(self.tmp.name) / "state.json")

    def tearDown(self):
        self.tmp.cleanup()

    def receiver(self, effects=None):
        return BatchReceiver(self.store, effects=effects)


class MixedBatchTest(ReceiverTestBase):
    def test_null_array_string_records_are_quarantined_good_ones_land_in_order(self):
        effects = RecordingEffects()
        receiver = self.receiver(effects)
        records = [
            event("evt-a"),
            None,  # 事故中的空值混入
            ["nested", "array"],  # 数组混入
            "bare-string",  # 字符串混入
            event("evt-b", kind="MERCHANT_COMMITMENT"),
            {"kind": "EVENT_PUBLISHED"},  # 缺字段的坏对象
            event("evt-c"),
        ]

        result = receiver.accept_batch("batch-1", records)

        self.assertEqual(result.accepted, [0, 4, 6])
        self.assertEqual(sorted(result.quarantined), [1, 2, 3, 5])
        self.assertEqual(result.duplicates, [])
        self.assertTrue(result.completed)

        ledger = receiver.ledger()
        self.assertEqual([e["event_id"] for e in ledger], ["evt-a", "evt-b", "evt-c"])
        # 账本保留批次内原始位置，好记录顺序不被坏记录打乱。
        self.assertEqual([e["index"] for e in ledger], [0, 4, 6])

        quarantined = receiver.quarantine()
        self.assertEqual([(q["batch_id"], q["index"]) for q in quarantined],
                         [("batch-1", 1), ("batch-1", 2), ("batch-1", 3), ("batch-1", 5)])
        shapes = {q["index"]: q["problems"][0]["code"] for q in quarantined}
        self.assertEqual(shapes[1], "bad_shape")  # null
        self.assertEqual(shapes[2], "bad_shape")  # 数组
        self.assertEqual(shapes[3], "bad_shape")  # 字符串
        self.assertEqual(shapes[5], "missing_field")  # 坏对象仍能定位字段

        # 坏记录从未触发任何副作用；好记录各触发一次。
        self.assertEqual(effects.calls, [])

    def test_non_array_batch_is_rejected_as_a_batch_error(self):
        receiver = self.receiver()
        with self.assertRaises(ValueError):
            receiver.accept_batch("batch-x", None)


class ReplayTest(ReceiverTestBase):
    def test_identical_redelivery_is_acknowledged_once(self):
        effects = RecordingEffects()
        receiver = self.receiver(effects)
        first = receiver.accept_batch("b1", [redeem("evt-1"), event("evt-2")])
        self.assertEqual(first.accepted, [0, 1])

        # 整批原样再投一次（运营重推）。
        second = receiver.accept_batch("b1", [redeem("evt-1"), event("evt-2")])
        self.assertEqual(second.accepted, [])
        self.assertEqual(second.duplicates, [0, 1])

        # 账本仍只有两条，权益核销副作用只发生过一次。
        self.assertEqual(len(receiver.ledger()), 2)
        self.assertEqual(effects.calls, [("write_off_benefit", "evt-1")])

    def test_same_event_id_with_conflicting_payload_does_not_overwrite(self):
        receiver = self.receiver()
        receiver.accept_batch("b1", [redeem("evt-1", quota=1)])

        conflict = redeem("evt-1", quota=99)
        result = receiver.accept_batch("b2", [conflict, event("evt-new")])

        self.assertEqual(result.quarantined, [0])
        self.assertEqual(result.accepted, [1])

        ledger = receiver.ledger()
        self.assertEqual(len(ledger), 2)
        original = next(e for e in ledger if e["event_id"] == "evt-1")
        self.assertEqual(original["payload"]["quota"], 1)  # 旧记录未被覆盖

        q = receiver.quarantine()[0]
        self.assertEqual(q["reason"], "duplicate_conflict")
        self.assertEqual(q["problems"][0]["code"], "duplicate_conflict")

        # 冲突投递再重复一次：结果稳定（依旧隔离，账本不变）。
        again = receiver.accept_batch("b3", [conflict])
        self.assertEqual(again.quarantined, [0])
        self.assertEqual(len(receiver.ledger()), 2)


class CrashRecoveryTest(ReceiverTestBase):
    def test_crash_after_reservation_does_not_retrigger_side_effect(self):
        effects = RecordingEffects(fail_event_id="evt-benefit")
        receiver = self.receiver(effects)

        with self.assertRaises(RuntimeError):
            receiver.accept_batch(
                "b1",
                [event("evt-a"), redeem("evt-benefit"), event("evt-c")],
            )

        # 副作用在预留落盘后恰好触发一次并中断；evt-a 已落账，evt-c 未处理。
        self.assertEqual(effects.calls, [("write_off_benefit", "evt-benefit")])
        self.assertEqual([e["event_id"] for e in receiver.ledger()],
                         ["evt-a", "evt-benefit"])

        # 新进程用同一存储恢复，重投批次（不带会崩的副作用）。
        recovered_effects = RecordingEffects()
        recovered = self.receiver(recovered_effects)
        result = recovered.accept_batch(
            "b1", [event("evt-a"), redeem("evt-benefit"), event("evt-c")]
        )

        self.assertEqual(result.duplicates, [0])
        self.assertEqual(result.reserved, [1])  # 待人工核对，而非重复核销
        self.assertEqual(result.accepted, [2])  # 从中断点继续处理后续
        self.assertFalse(result.completed)

        # 恢复过程中核销副作用一次都没有再触发。
        self.assertEqual(recovered_effects.calls, [])
        pending = recovered.pending_reconciliation()
        self.assertEqual([(p["event_id"], p["status"]) for p in pending],
                         [("evt-benefit", "reserved")])

        # 运营确认核销在中断前已完成：仅补登记，副作用仍不重放。
        reconcile_result = recovered.reconcile("b1", 1, effect_applied=True)
        self.assertTrue(reconcile_result["ok"])
        self.assertFalse(reconcile_result["replayed"])
        self.assertEqual(recovered_effects.calls, [])
        self.assertEqual(recovered.pending_reconciliation(), [])

        # 收口后批次完成；再投仍只确认一次。
        final = recovered.accept_batch(
            "b1", [event("evt-a"), redeem("evt-benefit"), event("evt-c")]
        )
        self.assertTrue(final.completed)
        self.assertEqual(final.duplicates, [0, 1, 2])
        self.assertEqual(recovered_effects.calls, [])

    def test_reconcile_with_effect_not_applied_replays_exactly_once(self):
        effects = RecordingEffects(fail_event_id="evt-benefit")
        receiver = self.receiver(effects)
        with self.assertRaises(RuntimeError):
            receiver.accept_batch("b1", [redeem("evt-benefit")])

        recovered_effects = RecordingEffects()
        recovered = self.receiver(recovered_effects)
        recovered.accept_batch("b1", [redeem("evt-benefit")])

        # 运营核对外部系统后确认核销从未发生：人工确认补触发，恰好一次。
        r = recovered.reconcile("b1", 0, effect_applied=False)
        self.assertTrue(r["replayed"])
        self.assertEqual(recovered_effects.calls, [("write_off_benefit", "evt-benefit")])
        self.assertEqual(recovered.pending_reconciliation(), [])

        # 再次重投不再触发。
        recovered.accept_batch("b1", [redeem("evt-benefit")])
        self.assertEqual(recovered_effects.calls, [("write_off_benefit", "evt-benefit")])


class QuarantineResubmitTest(ReceiverTestBase):
    def test_corrected_quarantine_record_reenters_at_original_position(self):
        receiver = self.receiver()
        records = [
            event("evt-a"),
            None,  # 位置 1 的坏记录
            event("evt-c"),
        ]
        receiver.accept_batch("b1", records)
        qid = receiver.quarantine()[0]["qid"]

        # 运营修正后带原始位置重新提交。
        corrected = event("evt-b", occurred_at="2026-09-20T10:00:00+08:00")
        r = receiver.resubmit_quarantine(qid, corrected)
        self.assertTrue(r["ok"])
        self.assertEqual((r["batch_id"], r["index"]), ("b1", 1))

        ledger = receiver.ledger()
        self.assertEqual([(e["batch_id"], e["index"], e["event_id"]) for e in ledger],
                         [("b1", 0, "evt-a"), ("b1", 1, "evt-b"), ("b1", 2, "evt-c")])
        self.assertEqual(receiver.quarantine(), [])

        # 原始批次位置现在记录为已确认（含已修正的位置 1）。
        again = receiver.accept_batch("b1", records)
        self.assertEqual(again.duplicates, [0, 1, 2])

    def test_still_invalid_correction_keeps_quarantine_open_with_new_problems(self):
        receiver = self.receiver()
        receiver.accept_batch("b1", [None])
        qid = receiver.quarantine()[0]["qid"]

        r = receiver.resubmit_quarantine(qid, {"kind": "EVENT_PUBLISHED"})
        self.assertFalse(r["ok"])
        codes = {p["code"] for p in r["problems"]}
        self.assertIn("missing_field", codes)
        self.assertEqual(len(receiver.quarantine()), 1)  # 隔离位仍开放

    def test_conflicting_correction_is_rejected_without_overwriting(self):
        receiver = self.receiver()
        receiver.accept_batch("b1", [redeem("evt-1", quota=1), None])
        qid = receiver.quarantine()[0]["qid"]

        r = receiver.resubmit_quarantine(qid, redeem("evt-1", quota=99))
        self.assertFalse(r["ok"])
        self.assertEqual(r["problems"][0]["code"], "duplicate_conflict")
        original = next(e for e in receiver.ledger() if e["event_id"] == "evt-1")
        self.assertEqual(original["payload"]["quota"], 1)
        self.assertEqual(len(receiver.quarantine()), 1)

    def test_state_survives_restart_from_disk(self):
        receiver = self.receiver()
        receiver.accept_batch("b1", [event("evt-a"), None, event("evt-c")])

        reopened = self.receiver()
        self.assertEqual([e["event_id"] for e in reopened.ledger()], ["evt-a", "evt-c"])
        q = reopened.quarantine()
        self.assertEqual([item["index"] for item in q], [1])
        # 隔离记录里保留原始载荷供运营修正。
        self.assertIsNone(q[0]["record"])

        corrected = json.loads(json.dumps(event("evt-b")))
        r = reopened.resubmit_quarantine(q[0]["qid"], corrected)
        self.assertTrue(r["ok"])
        self.assertEqual([e["event_id"] for e in reopened.ledger()],
                         ["evt-a", "evt-b", "evt-c"])


if __name__ == "__main__":
    unittest.main()
