"""可靠的赛事消费事件批量接收路径。

设计要点（对应当班故障的修复目标）：

* 校验不过的记录进入 *隔离区*，保留批次号与批次内原始位置；
  好记录不受影响，按批次内原始顺序落账。
* 以 event_id 做内容指纹幂等：完全重放只确认一次、不重复落账、
  不重复触发副作用；同 event_id 内容冲突时拒绝并隔离，绝不覆盖旧记录。
* 副作用（权益核销、投诉结案）执行前先把记录持久化为 reserved；
  进程中断后重投/续跑时，reserved 记录只进入人工核对队列，
  副作用绝不会被自动重复触发。
* 隔离记录修正后可带原始批次位置重新提交，按原位置插回账本。

状态全部保存在单个 JSON 文件中，每次写入都走临时文件 + 原子替换。
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .event_consumer_guard import Problem, validate_event

# 事件类型 -> 外部副作用名称。副作用通过 effects 对象的同名方法触发。
SIDE_EFFECTS: dict[str, str] = {
    "BENEFIT_REDEEMED": "write_off_benefit",
    "REMEDY_SETTLED": "close_complaint",
}

# 记录状态机：reserved（副作用已预留，待确认）-> done；quarantine 独立维护。
STATUS_RESERVED = "reserved"
STATUS_DONE = "done"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_fingerprint(record: dict[str, Any]) -> str:
    """整条事件内容的稳定指纹：记录完全一致时指纹一致。"""
    blob = json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class BatchResult:
    """一次批次受理的确定结果。"""

    batch_id: str
    accepted: list[int] = field(default_factory=list)
    quarantined: list[int] = field(default_factory=list)
    duplicates: list[int] = field(default_factory=list)
    reserved: list[int] = field(default_factory=list)
    completed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "accepted": list(self.accepted),
            "quarantined": list(self.quarantined),
            "duplicates": list(self.duplicates),
            "reserved": list(self.reserved),
            "completed": self.completed,
        }


class BatchReceiver:
    """事件批量接收器；一个实例对应磁盘上的一份接收状态。"""

    def __init__(self, store_path: str | os.PathLike[str], effects: Any | None = None) -> None:
        self._path = os.fspath(store_path)
        self._effects = effects
        self._state = self._load()

    # ------------------------------------------------------------------ 持久化

    def _load(self) -> dict[str, Any]:
        if os.path.exists(self._path):
            with open(self._path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        return {"batches": {}, "records": {}, "ledger": [], "quarantine": []}

    def _persist(self) -> None:
        directory = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".batch-state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self._state, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    # ------------------------------------------------------------------ 查询

    def ledger(self) -> list[dict[str, Any]]:
        """已落账事件，按 (批次, 批次内原始位置) 排序返回。"""
        return [dict(entry) for entry in sorted(self._state["ledger"], key=lambda e: (e["batch_id"], e["index"]))]

    def quarantine(self, *, include_resolved: bool = False) -> list[dict[str, Any]]:
        items = self._state["quarantine"]
        if include_resolved:
            return [dict(item) for item in items]
        return [dict(item) for item in items if item["status"] == "open"]

    def pending_reconciliation(self) -> list[dict[str, Any]]:
        """副作用已预留、崩溃恢复后需要人工收口的记录。"""
        return [
            dict(entry)
            for entry in self._state["ledger"]
            if entry["status"] == STATUS_RESERVED
        ]

    # ------------------------------------------------------------- 批次受理主流程

    def accept_batch(self, batch_id: str, records: Any) -> BatchResult:
        """受理一个批次；重复调用同一批次是幂等的续跑/重放。

        records 应为 JSON 数组；非数组整体不可解析时抛 ValueError
        （调用方拿不到批次内位置，无法逐条隔离）。
        """
        if not isinstance(records, list):
            raise ValueError(
                f"批次必须是 JSON 数组，实际收到 {type(records).__name__}"
            )

        result = BatchResult(batch_id=batch_id)
        batch = self._state["batches"].setdefault(
            batch_id, {"received_at": _utc_now(), "size": len(records), "status": "open"}
        )

        for index, raw in enumerate(records):
            key = f"{batch_id}:{index}"
            prior = self._state["records"].get(key)
            if prior is not None:
                # 崩溃续跑或完全重投：首投结果即确定结果，只汇总不重做。
                self._summarize_prior(prior, result, index, batch_id, is_prior=True)
                continue
            outcome = self._admit_one(batch_id, index, raw)
            self._state["records"][key] = outcome
            self._persist()
            self._summarize_prior(outcome, result, index, batch_id, is_prior=False)

        open_reserved = any(
            entry["status"] == STATUS_RESERVED
            for entry in self._state["ledger"]
            if entry["batch_id"] == batch_id
        )
        batch["status"] = "open" if open_reserved else "complete"
        result.completed = not open_reserved
        self._persist()
        return result

    def _summarize_prior(
        self,
        outcome: dict[str, Any],
        result: BatchResult,
        index: int,
        batch_id: str,
        *,
        is_prior: bool,
    ) -> None:
        bucket = outcome["outcome"]
        if bucket == "accepted":
            if is_prior:
                # 重投时按账本当前状态归类：已完成→重复确认；中断在预留→待核对。
                entry = self._ledger_entry(batch_id, index)
                if entry is not None and entry["status"] == STATUS_RESERVED:
                    result.reserved.append(index)
                else:
                    result.duplicates.append(index)
            else:
                result.accepted.append(index)
        elif bucket == "duplicate":
            result.duplicates.append(index)
        elif bucket == "quarantine":
            result.quarantined.append(index)
        elif bucket == "reserved":
            result.reserved.append(index)

    def _admit_one(self, batch_id: str, index: int, raw: Any) -> dict[str, Any]:
        """处理单条记录并落盘相应状态（隔离/账本/副作用预留）。"""
        problems = validate_event(raw)
        if problems:
            return self._quarantine(batch_id, index, raw, problems, reason="validation_failed")

        event_id = raw["event_id"]
        fingerprint = canonical_fingerprint(raw)
        accepted = self._accepted_event(event_id)

        if accepted is not None:
            if accepted["fingerprint"] == fingerprint:
                if accepted["status"] == STATUS_RESERVED:
                    # 崩溃恢复：副作用预留后中断，绝不重放，转人工核对。
                    return {"outcome": "reserved", "event_id": event_id}
                # 同一事件的完全重放：确认既有记录即可，只确认一次。
                return {"outcome": "duplicate", "event_id": event_id}
            # 内容冲突：不覆盖旧记录，隔离本次投递并给出可解释结论。
            problems = [
                Problem(
                    field="event_id",
                    code="duplicate_conflict",
                    message=(
                        f"event_id={event_id} 已有内容不同的记录，"
                        "拒绝覆盖；既有记录保持不变"
                    ),
                )
            ]
            return self._quarantine(
                batch_id, index, raw, problems, reason="duplicate_conflict", event_id=event_id
            )

        # 相同的冲突投递此前已隔离过：同样稳定地拒绝一次。
        if self._known_conflict(event_id, fingerprint):
            return {"outcome": "quarantine", "event_id": event_id, "reason": "duplicate_conflict"}

        return self._insert_new_event(batch_id, index, raw, fingerprint)

    def _insert_new_event(
        self, batch_id: str, index: int, record: dict[str, Any], fingerprint: str
    ) -> dict[str, Any]:
        """把一条通过去重的新事件写入账本并触发其副作用。"""
        effect_name = SIDE_EFFECTS.get(record["kind"])
        entry = {
            "batch_id": batch_id,
            "index": index,
            "event_id": record["event_id"],
            "kind": record["kind"],
            "occurred_at": record["occurred_at"],
            "subject_id": record["subject_id"],
            "payload": record["payload"],
            "fingerprint": fingerprint,
            "effect": effect_name,
            "effect_status": "pending" if effect_name else "none",
            "status": STATUS_RESERVED if effect_name else STATUS_DONE,
            "recorded_at": _utc_now(),
        }
        self._state["ledger"].append(entry)
        self._persist()

        if effect_name is None:
            return {"outcome": "accepted", "event_id": entry["event_id"]}

        # 先持久化 reserved 再触发副作用；崩溃/异常都不会导致重复触发。
        self._invoke_effect(effect_name, entry)
        entry["status"] = STATUS_DONE
        entry["effect_status"] = "done"
        self._persist()
        return {"outcome": "accepted", "event_id": entry["event_id"]}

    def _invoke_effect(self, effect_name: str, entry: dict[str, Any]) -> None:
        if self._effects is None:
            return
        handler: Callable[[dict[str, Any]], None] = getattr(self._effects, effect_name)
        handler(self._event_view(entry))

    @staticmethod
    def _event_view(entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "event_id": entry["event_id"],
            "kind": entry["kind"],
            "occurred_at": entry["occurred_at"],
            "subject_id": entry["subject_id"],
            "payload": entry["payload"],
        }

    # ------------------------------------------------------------- 隔离与重提

    def _quarantine(
        self,
        batch_id: str,
        index: int,
        raw: Any,
        problems: list[Problem],
        *,
        reason: str,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        item = {
            "qid": f"Q{len(self._state['quarantine']) + 1:04d}",
            "batch_id": batch_id,
            "index": index,
            "record": raw,
            "problems": [p.as_dict() for p in problems],
            "reason": reason,
            "event_id": event_id,
            "status": "open",
            "quarantined_at": _utc_now(),
            "resolved_at": None,
        }
        self._state["quarantine"].append(item)
        return {
            "outcome": "quarantine",
            "qid": item["qid"],
            "reason": reason,
            "event_id": event_id,
        }

    def resubmit_quarantine(self, qid: str, corrected_record: Any) -> dict[str, Any]:
        """修正隔离记录后，带着原始批次位置重新提交。

        校验仍不通过时保留隔离位并就地更新问题；通过后按原始 (批次, 位置)
        插回账本并关闭隔离位。返回包含 ok/qid/位置/剩余问题 的确定结果。
        """
        item = self._find_open_quarantine(qid)
        batch_id, index = item["batch_id"], item["index"]

        problems = validate_event(corrected_record)
        outcome: dict[str, Any] | None = None
        if not problems:
            event_id = corrected_record["event_id"]
            fingerprint = canonical_fingerprint(corrected_record)
            accepted = self._accepted_event(event_id)
            if accepted is not None and accepted["fingerprint"] == fingerprint:
                # 与既有记录完全一致：直接确认，不重复落账。
                outcome = {"outcome": "duplicate", "event_id": event_id}
            elif accepted is not None:
                problems = [
                    Problem(
                        field="event_id",
                        code="duplicate_conflict",
                        message=f"event_id={event_id} 已有内容不同的记录，拒绝覆盖",
                    )
                ]
                item["reason"] = "duplicate_conflict"
                item["event_id"] = event_id

        if problems:
            item["record"] = corrected_record
            item["problems"] = [p.as_dict() for p in problems]
            item["quarantined_at"] = _utc_now()
            self._persist()
            return {
                "ok": False,
                "qid": qid,
                "batch_id": batch_id,
                "index": index,
                "problems": [p.as_dict() for p in problems],
            }

        if outcome is None:
            outcome = self._insert_new_event(batch_id, index, corrected_record, fingerprint)
        item["status"] = "resolved"
        item["resolved_at"] = _utc_now()
        self._state["records"][f"{batch_id}:{index}"] = outcome
        self._refresh_batch_status(batch_id)
        self._persist()
        return {
            "ok": True,
            "qid": qid,
            "batch_id": batch_id,
            "index": index,
            "outcome": outcome["outcome"],
        }

    # ------------------------------------------------------------- 恢复与核对

    def reconcile(self, batch_id: str, index: int, *, effect_applied: bool) -> dict[str, Any]:
        """对崩溃恢复后停在 reserved 的记录人工收口。

        effect_applied=True 表示副作用在中断前已完成，仅补登记，绝不重放；
        effect_applied=False 表示确认未发生过，此刻在人工确认下补触发一次。
        """
        entry = self._ledger_entry(batch_id, index)
        if entry is None or entry["status"] != STATUS_RESERVED:
            raise ValueError(f"记录 {batch_id}:{index} 不在待核对(reserved)状态")
        if not effect_applied:
            self._invoke_effect(entry["effect"], entry)
        entry["status"] = STATUS_DONE
        entry["effect_status"] = "done"
        self._state["records"][f"{batch_id}:{index}"] = {
            "outcome": "accepted",
            "event_id": entry["event_id"],
        }
        self._refresh_batch_status(batch_id)
        self._persist()
        return {
            "ok": True,
            "batch_id": batch_id,
            "index": index,
            "effect": entry["effect"],
            "replayed": not effect_applied,
        }

    def _refresh_batch_status(self, batch_id: str) -> None:
        open_reserved = any(
            e["status"] == STATUS_RESERVED
            for e in self._state["ledger"]
            if e["batch_id"] == batch_id
        )
        if batch_id in self._state["batches"]:
            self._state["batches"][batch_id]["status"] = "open" if open_reserved else "complete"

    # ------------------------------------------------------------------ 索引

    def _accepted_event(self, event_id: str) -> dict[str, Any] | None:
        for entry in self._state["ledger"]:
            if entry["event_id"] == event_id and entry["status"] in (STATUS_DONE, STATUS_RESERVED):
                return entry
        return None

    def _known_conflict(self, event_id: str, fingerprint: str) -> bool:
        for item in self._state["quarantine"]:
            if (
                item["status"] == "open"
                and item.get("reason") == "duplicate_conflict"
                and item.get("event_id") == event_id
                and isinstance(item["record"], dict)
                and canonical_fingerprint(item["record"]) == fingerprint
            ):
                return True
        return False

    def _ledger_entry(self, batch_id: str, index: int) -> dict[str, Any] | None:
        for entry in self._state["ledger"]:
            if entry["batch_id"] == batch_id and entry["index"] == index:
                return entry
        return None

    def _find_open_quarantine(self, qid: str) -> dict[str, Any]:
        for item in self._state["quarantine"]:
            if item["qid"] == qid and item["status"] == "open":
                return item
        raise ValueError(f"隔离记录 {qid} 不存在或已处理")
