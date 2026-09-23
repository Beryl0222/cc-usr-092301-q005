"""可靠批量接收路径测试：

覆盖空值/混合批次、完全重放、冲突重放、崩溃恢复（副作用恰好一次）、
隔离记录修正后带原始位置重新入队。
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.batch_receiver import (
    REASON_CONFLICT,
    REASON_DELIVERY,
    REASON_INVALID,
    BatchReceiver,
    SideEffectHandler,
)
from src.event_consumer_guard import validate_event_detailed


def event(event_id, kind="EVENT_PUBLISHED", **payload):
    return {
        "event_id": event_id,
        "kind": kind,
        "occurred_at": "2026-09-20T09:00:00+08:00",
        "subject_id": "subject-1",
        "payload": payload or {"note": "ok"},
    }


def redeem(event_id, amount=1):
    return event(event_id, "BENEFIT_REDEEMED", benefit_id="b-1", redeem_amount=amount)


def remedy(event_id):
    return event(event_id, "REMEDY_SETTLED", complaint_id="c-1", settle_amount=2)


class RecordingSideEffects(SideEffectHandler):
    def __init__(self):
        self.calls = []

    def benefit_redeemed(self, event_id, record):
        self.calls.append(("redeem", event_id))

    def complaint_opened(self, event_id, record):
        self.calls.append(("open", event_id))

    def complaint_closed(self, event_id, record):
        self.calls.append(("close", event_id))


class CrashInSideEffect(SideEffectHandler):
    """在指定事件的核销执行到一半时“进程被杀”。"""

    def __init__(self, crash_event_id):
        self.crash_event_id = crash_event_id
        self.calls = []

    def benefit_redeemed(self, event_id, record):
        self.calls.append(event_id)
        if event_id == self.crash_event_id:
            raise SystemExit("simulated kill mid-side-effect")


class MixedBatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "state.db")
        self.addCleanup(self.tmp.cleanup)

    def receiver(self, handler=None):
        return BatchReceiver(self.path, side_effects=handler)

    def test_null_array_string_records_are_quarantined_good_ones_land_in_order(self):
        good1 = event("e-1")
        good2 = redeem("e-2")
        good3 = event("e-3")
        batch = [None, ["array"], "string", good1, good2, {"kind": "EVENT_PUBLISHED"}, good3]

        effects = RecordingSideEffects()
        with self.receiver(effects) as rx:
            result = rx.ingest(batch, batch_id="b1")

            # 3 条坏记录（null/数组/字符串 + 缺字段对象）
            self.assertEqual(result.committed, ["e-1", "e-2", "e-3"])
            self.assertEqual(len(result.quarantined), 4)
            self.assertTrue(all(t.reason == REASON_INVALID for t in result.quarantined))
            # 好记录按原始顺序落账
            ledger = rx.ledger()
            self.assertEqual([r["event_id"] for r in ledger], ["e-1", "e-2", "e-3"])
            self.assertEqual([r["position"] for r in ledger], [3, 4, 6])
        # 核销副作用只对 BENEFIT_REDEEMED 触发一次
        self.assertEqual(effects.calls, [("redeem", "e-2")])

    def test_quarantine_problems_are_structured_and_located(self):
        with self.receiver() as rx:
            rx.ingest([None], batch_id="bx")
            ticket = rx.list_quarantine()[0]
        self.assertEqual(ticket.reason, REASON_INVALID)
        self.assertEqual(ticket.problems[0].code, "not_an_object")
        self.assertIsNone(ticket.problems[0].field)
        # 票据可 JSON 序列化给运营
        json.dumps(ticket.to_dict(), ensure_ascii=False)


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.rx = BatchReceiver(":memory:", side_effects=RecordingSideEffects())

    def tearDown(self):
        self.rx.close()

    def test_identical_replay_confirmed_once_no_duplicate_side_effect(self):
        first = redeem("e-dup")
        r1 = self.rx.ingest([first], batch_id="b1")
        r2 = self.rx.ingest([dict(first)], batch_id="b2")  # 同内容重投
        # 键顺序不同但内容相同，也算完全重放
        shuffled = {
            "payload": first["payload"],
            "subject_id": first["subject_id"],
            "occurred_at": first["occurred_at"],
            "kind": first["kind"],
            "event_id": first["event_id"],
        }
        r3 = self.rx.ingest([shuffled], batch_id="b3")

        self.assertEqual(r1.committed, ["e-dup"])
        self.assertEqual(r2.replayed, ["e-dup"])
        self.assertEqual(r2.committed, [])
        self.assertEqual(r3.replayed, ["e-dup"])
        # 台账只有一条；核销只执行一次
        self.assertEqual(len(self.rx.ledger()), 1)
        self.assertEqual(self.rx._side_effects.calls, [("redeem", "e-dup")])

    def test_conflicting_replay_does_not_overwrite(self):
        original = redeem("e-x", amount=1)
        conflict = redeem("e-x", amount=999)
        self.rx.ingest([original], batch_id="b1")
        r2 = self.rx.ingest([conflict], batch_id="b2")

        self.assertEqual(r2.committed, [])
        self.assertEqual(len(r2.quarantined), 1)
        ticket = r2.quarantined[0]
        self.assertEqual(ticket.reason, REASON_CONFLICT)
        self.assertEqual(ticket.problems[0].code, "conflicting_replay")
        # 旧记录保持不变
        landed = self.rx.ledger()[0]
        self.assertEqual(landed["record"]["payload"]["redeem_amount"], 1)
        # 核销仍然只执行一次
        self.assertEqual(self.rx._side_effects.calls, [("redeem", "e-x")])


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "state.db")
        self.addCleanup(self.tmp.cleanup)

    def test_resume_after_mid_sideeffect_crash_does_not_repeat_side_effect(self):
        batch = [redeem("e-1"), redeem("e-2"), remedy("e-3")]

        # 第一次运行：在 e-2 核销执行后、落账确认前“进程被杀”。
        h1 = CrashInSideEffect("e-2")
        with self.assertRaises(SystemExit):
            BatchReceiver(self.path, side_effects=h1).ingest(batch, batch_id="b1")
        self.assertEqual(h1.calls, ["e-1", "e-2"])

        # 新进程恢复：e-1 已完成；e-2 认领存在但未确认 → 不重复核销，转 delivery 隔离；
        # e-3 继续处理。
        h2 = RecordingSideEffects()
        rx = BatchReceiver(self.path, side_effects=h2)
        result = rx.resume("b1")

        self.assertEqual(result.committed, ["e-3"])
        self.assertEqual([t.event_id for t in result.quarantined], ["e-2"])
        self.assertEqual(result.quarantined[0].reason, REASON_DELIVERY)
        # 恢复进程没有再次核销 e-1/e-2，只结案 e-3
        self.assertEqual(h2.calls, [("close", "e-3")])
        # 再 resume 一次结果稳定（没有新增副作用、没有重复隔离票据）
        before = len(rx.list_quarantine())
        result2 = rx.resume("b1")
        self.assertEqual(result2.committed, [])
        self.assertEqual(result2.replayed, [])
        self.assertEqual(len(rx.list_quarantine()), before)
        rx.close()

    def test_resume_after_crash_between_records_processes_remaining_in_order(self):
        batch = [event("a"), event("b"), event("c")]
        # 在处理到第 2 条之前崩溃：直接模拟只持久化批次、未处理
        rx0 = BatchReceiver(self.path)
        rx0.ingest([], batch_id="b1")  # 建批次
        for pos, rec in enumerate(batch):
            rx0._insert_record_row("b1", pos, rec, "2026-09-20T00:00:00+00:00")
        rx0._conn.commit()
        rx0.close()

        rx = BatchReceiver(self.path)
        result = rx.resume("b1")
        self.assertEqual(result.committed, ["a", "b", "c"])
        self.assertEqual(
            [r["position"] for r in rx.ledger()], [0, 1, 2]
        )
        # 幂等：再 resume 不产生任何新结果
        self.assertEqual(rx.resume("b1").committed, [])
        rx.close()


class RequeueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "state.db")
        self.addCleanup(self.tmp.cleanup)

    def test_corrected_quarantine_record_requeued_at_original_position(self):
        bad = {"kind": "BENEFIT_REDEEMED"}  # 缺字段，位于批次第 2 条
        good = event("e-ok")
        effects = RecordingSideEffects()
        rx = BatchReceiver(self.path, side_effects=effects)
        result = rx.ingest([event("e-0"), bad, event("e-2")], batch_id="b1")
        ticket = next(t for t in result.quarantined if t.raw is bad or t.original_position == 1)
        self.assertEqual(ticket.original_position, 1)

        corrected = redeem("e-fixed")
        rq = rx.requeue(ticket.ticket_id, corrected)
        self.assertEqual(rq.committed, ["e-fixed"])

        ledger = rx.ledger()
        ids = [r["event_id"] for r in ledger]
        self.assertEqual(ids, ["e-0", "e-fixed", "e-2"])
        fixed_row = next(r for r in ledger if r["event_id"] == "e-fixed")
        # 带着原始位置重新落账
        self.assertEqual(fixed_row["batch_id"], "b1")
        self.assertEqual(fixed_row["position"], 1)
        # 旧票据已解决，隔离区清空
        self.assertEqual(rx.list_quarantine(), [])
        self.assertEqual(effects.calls, [("redeem", "e-fixed")])
        rx.close()

    def test_requeue_still_invalid_opens_new_ticket(self):
        rx = BatchReceiver(self.path)
        result = rx.ingest([None], batch_id="b1")
        ticket = result.quarantined[0]
        rq = rx.requeue(ticket.ticket_id, ["still", "bad"])
        self.assertEqual(rq.committed, [])
        self.assertEqual(len(rq.quarantined), 1)
        self.assertEqual(rq.quarantined[0].reason, REASON_INVALID)
        # 新票据仍保留原始位置信息
        self.assertEqual(rq.quarantined[0].original_position, 0)
        # 旧票据已标记解决，未解决票据只剩新的一张
        open_tickets = rx.list_quarantine()
        self.assertEqual(len(open_tickets), 1)
        self.assertNotEqual(open_tickets[0].ticket_id, ticket.ticket_id)
        rx.close()

    def test_requeue_after_delivery_interruption_lands_without_second_effect(self):
        # e-2 核销已执行但确认前崩溃 → delivery 隔离；对账确认核销已发生，
        # 凭票据重提同一条记录：应补落账但不再次核销。
        batch = [redeem("e-1"), redeem("e-2")]
        h1 = CrashInSideEffect("e-2")
        with self.assertRaises(SystemExit):
            BatchReceiver(self.path, side_effects=h1).ingest(batch, batch_id="b1")

        h2 = RecordingSideEffects()
        rx = BatchReceiver(self.path, side_effects=h2)
        result = rx.resume("b1")
        ticket = result.quarantined[0]
        self.assertEqual(ticket.reason, REASON_DELIVERY)

        rq = rx.requeue(ticket.ticket_id, redeem("e-2"))
        self.assertEqual(rq.committed, ["e-2"])
        # 关键：核销没有第二次执行
        self.assertEqual(h2.calls, [])
        ledger_ids = [r["event_id"] for r in rx.ledger()]
        self.assertEqual(ledger_ids, ["e-1", "e-2"])
        rx.close()


class SideEffectFailureTest(unittest.TestCase):
    def test_failing_hook_quarantines_delivery_and_continues_batch(self):
        class FailOpen(SideEffectHandler):
            def __init__(self):
                self.calls = []

            def complaint_opened(self, event_id, record):
                self.calls.append(event_id)
                raise RuntimeError("downstream 500")

        effects = FailOpen()
        rx = BatchReceiver(":memory:", side_effects=effects)
        opened = event("c-1", "COMPLAINT_OPENED", complaint_id="c-1")
        result = rx.ingest([opened, event("e-after")], batch_id="b1")
        self.assertEqual([t.event_id for t in result.quarantined], ["c-1"])
        self.assertEqual(result.quarantined[0].reason, REASON_DELIVERY)
        self.assertEqual(result.committed, ["e-after"])
        # 落账被阻止
        self.assertEqual([r["event_id"] for r in rx.ledger()], ["e-after"])
        rx.close()


if __name__ == "__main__":
    unittest.main()
