"""可靠的赛事消费事件批量接收路径。

设计要点
========

* 坏记录（任意 JSON 形状）进入隔离区，不影响同批好记录；好记录按批次内原始顺序落账。
* 同一 ``event_id`` 的**完全重放**（内容规范化后一致）是幂等重发，只确认一次、
  不重复触发副作用；**内容冲突**的重发不覆盖已落账记录，转入隔离区由运营裁决。
* 外部副作用（权益核销、投诉开案/结案）采用「认领 → 执行 → 确认」两段式：

  1. 事务里写入副作用认领并把记录置为 ``delivering``，提交（认领持久化）；
  2. 事务外执行副作用；
  3. 再开事务写入台账并置为 ``committed``。

  进程在第 2 步前后中断时，恢复逻辑看到 ``delivering`` 且认领已存在，
  **绝不会第二次执行副作用**，而是把记录转入隔离区（原因 ``delivery``）
  交运营对账；对账确认副作用已发生后，凭票据 :meth:`BatchReceiver.requeue`
  重提即可补落账，且已认领的副作用不会再次触发。

* 每条记录独立提交，``resume`` 从断点继续，已完成记录全部幂等跳过。
* 隔离记录修正后凭票据重新提交，沿用其在原始批次中的位置。

副作用通过 :class:`SideEffectHandler` 注入。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from .event_consumer_guard import Problem, validate_event_detailed

# 记录在批次处理生命周期中的状态。
STATUS_PENDING = "pending"
STATUS_DELIVERING = "delivering"  # 副作用认领已持久化、等待确认
STATUS_COMMITTED = "committed"
STATUS_QUARANTINED = "quarantined"

# 隔离原因。
REASON_INVALID = "invalid"            # 未通过结构校验
REASON_CONFLICT = "conflict"          # event_id 已落账但内容不一致
REASON_DELIVERY = "delivery"          # 副作用执行时进程中断，需人工对账

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id    TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    last_pos    INTEGER NOT NULL DEFAULT -1
);
CREATE TABLE IF NOT EXISTS records (
    batch_id     TEXT NOT NULL,
    position     INTEGER NOT NULL,
    event_id     TEXT,
    status       TEXT NOT NULL,
    raw_json     TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (batch_id, position)
);
CREATE TABLE IF NOT EXISTS ledger (
    event_id     TEXT PRIMARY KEY,
    batch_id     TEXT NOT NULL,
    position     INTEGER NOT NULL,
    raw_json     TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS side_effect_claims (
    event_id  TEXT NOT NULL,
    effect    TEXT NOT NULL,
    PRIMARY KEY (event_id, effect)
);
CREATE TABLE IF NOT EXISTS quarantine (
    ticket_id         TEXT PRIMARY KEY,
    original_batch_id TEXT NOT NULL,
    original_position INTEGER NOT NULL,
    event_id          TEXT,
    reason            TEXT NOT NULL,
    raw_json          TEXT NOT NULL,
    problems_json     TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    resolved_at       TEXT
);
"""


@dataclass
class QuarantineTicket:
    """隔离区票据：修正载荷后凭它重新提交。"""

    ticket_id: str
    original_batch_id: str
    original_position: int
    event_id: str | None
    reason: str
    raw: Any
    problems: list[Problem] = field(default_factory=list)
    created_at: str = ""
    resolved_at: str | None = None

    @property
    def original_index(self) -> int:
        """原始批次中的 0 基位置。"""
        return self.original_position

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticket_id": self.ticket_id,
            "original_batch_id": self.original_batch_id,
            "original_position": self.original_position,
            "event_id": self.event_id,
            "reason": self.reason,
            "raw": self.raw,
            "problems": [
                {"field": p.field, "code": p.code, "message": p.message}
                for p in self.problems
            ],
            "created_at": self.created_at,
            "resolved_at": self.resolved_at,
        }


@dataclass
class BatchResult:
    """一次（含恢复、重提的）处理推进结果。"""

    batch_id: str
    committed: list[str] = field(default_factory=list)
    replayed: list[str] = field(default_factory=list)
    quarantined: list[QuarantineTicket] = field(default_factory=list)
    resumed: bool = False

    @property
    def ok(self) -> bool:
        return not self.quarantined


class SideEffectHandler:
    """副作用注入点。

    每个 hook 在副作用认领持久化之后、落账确认之前执行，
    全进程对同一事件的同一副作用最多执行一次。
    hook 抛错或进程在此中断，恢复时不会自动重试（避免重复核销/重复结案），
    记录转入原因 ``delivery`` 的隔离区等待对账。
    """

    def benefit_redeemed(self, event_id: str, record: dict) -> None:
        """BENEFIT_REDEEMED：权益核销。"""

    def complaint_opened(self, event_id: str, record: dict) -> None:
        """COMPLAINT_OPENED：投诉立案。"""

    def complaint_closed(self, event_id: str, record: dict) -> None:
        """REMEDY_SETTLED：投诉结案与补偿。"""


_EFFECT_HOOKS: dict[str, tuple[str, str]] = {
    # kind -> (副作用名, handler 方法名)
    "BENEFIT_REDEEMED": ("benefit_redeem", "benefit_redeemed"),
    "COMPLAINT_OPENED": ("complaint_open", "complaint_opened"),
    "REMEDY_SETTLED": ("complaint_close", "complaint_closed"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(raw: Any) -> str:
    """事件内容的规范字节形式，用于判定完全重放。

    采用 sort_keys 的 JSON 文本：与到达时键顺序、空白无关，
    内容相同即视为同一事件的重发。
    """
    return json.dumps(raw, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class BatchReceiver:
    """批量接收器；一个实例对应一个 sqlite 库文件。"""

    def __init__(
        self,
        store_path: str | Path = ":memory:",
        side_effects: SideEffectHandler | None = None,
    ) -> None:
        self._side_effects = side_effects or SideEffectHandler()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(store_path)
        self._conn.row_factory = sqlite3.Row
        if store_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- 基础工具 --------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "BatchReceiver":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @staticmethod
    def _row_to_ticket(row: sqlite3.Row) -> QuarantineTicket:
        problems = [
            Problem(field=p["field"], code=p["code"], message=p["message"])
            for p in json.loads(row["problems_json"])
        ]
        return QuarantineTicket(
            ticket_id=row["ticket_id"],
            original_batch_id=row["original_batch_id"],
            original_position=row["original_position"],
            event_id=row["event_id"],
            reason=row["reason"],
            raw=json.loads(row["raw_json"]),
            problems=problems,
            created_at=row["created_at"],
            resolved_at=row["resolved_at"],
        )

    # -- 接收与恢复 ------------------------------------------------------

    def ingest(
        self, records: Iterable[Any], batch_id: str | None = None
    ) -> BatchResult:
        """持久化一个批次并立即处理；处理结果可由 :meth:`resume` 继续。"""
        records = list(records)
        with self._lock:
            if batch_id is None:
                batch_id = f"batch-{uuid.uuid4().hex}"
            now = _now()
            existed = self._conn.execute(
                "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if existed is not None:
                raise ValueError(
                    f"批次 {batch_id!r} 已接收过；继续处理请使用 resume(batch_id)"
                )
            self._conn.execute(
                "INSERT INTO batches(batch_id, created_at, updated_at, last_pos) "
                "VALUES (?, ?, ?, ?)",
                (batch_id, now, now, -1),
            )
            for pos, raw in enumerate(records):
                self._insert_record_row(batch_id, pos, raw, now)
            self._conn.commit()
            return self.resume(batch_id)

    def _insert_record_row(
        self, batch_id: str, pos: int, raw: Any, now: str
    ) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO records(batch_id, position, event_id, status, raw_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                batch_id,
                pos,
                raw.get("event_id") if isinstance(raw, dict) and isinstance(raw.get("event_id"), str) else None,
                STATUS_PENDING,
                json.dumps(raw, ensure_ascii=False),
                now,
            ),
        )

    def resume(self, batch_id: str) -> BatchResult:
        """继续处理指定批次；对已完成批次是安全的（全部幂等跳过）。

        每条记录独立提交，进程在任意位置中断后重跑本方法即可从断点继续。
        """
        result = BatchResult(batch_id=batch_id, resumed=True)
        with self._lock:
            batch = self._conn.execute(
                "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise KeyError(f"未知批次 {batch_id!r}")

            rows = self._conn.execute(
                "SELECT position, status, raw_json FROM records "
                "WHERE batch_id=? ORDER BY position",
                (batch_id,),
            ).fetchall()

            for row in rows:
                status = row["status"]
                if status in (STATUS_COMMITTED, STATUS_QUARANTINED):
                    continue
                pos = row["position"]
                raw = json.loads(row["raw_json"])
                if status == STATUS_DELIVERING:
                    # 副作用认领已持久化但未确认：绝不重放，转人工对账。
                    ticket = self._quarantine(
                        batch_id,
                        pos,
                        raw,
                        [
                            Problem(
                                field=None,
                                code="delivery_interrupted",
                                message=(
                                    "副作用执行期间处理中断，为避免重复核销/重复结案，"
                                    "记录暂停落账，请对账后凭票据重新提交"
                                ),
                            )
                        ],
                        REASON_DELIVERY,
                    )
                    result.quarantined.append(ticket)
                    continue
                self._process_one(batch_id, pos, raw, result)

            self._touch_batch(batch_id)
            return result

    # -- 单记录处理 ------------------------------------------------------

    def _process_one(
        self, batch_id: str, pos: int, raw: Any, result: BatchResult
    ) -> None:
        report = validate_event_detailed(raw)
        if not report.valid:
            result.quarantined.append(
                self._quarantine(batch_id, pos, raw, report.problems, REASON_INVALID)
            )
            return

        event_id = raw["event_id"]
        kind = raw["kind"]
        canonical = _canonical(raw)

        # 重放判定（短事务，只读+状态推进）。
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT raw_json FROM ledger WHERE event_id=?", (event_id,)
                ).fetchone()
                if existing is not None:
                    if _canonical(json.loads(existing["raw_json"])) == canonical:
                        # 完全重放：确认即可，绝不再次触发副作用。
                        self._set_status(batch_id, pos, STATUS_COMMITTED, event_id)
                        self._conn.commit()
                        result.replayed.append(event_id)
                        return
                    self._conn.commit()
                    # 内容冲突：不覆盖旧记录，转入隔离。
                    conflict_problems = [
                        Problem(
                            field="event_id",
                            code="conflicting_replay",
                            message=(
                                f"event_id={event_id} 已落账但内容不一致，"
                                "保留旧记录，本次投递转入隔离区"
                            ),
                        )
                    ]
                    result.quarantined.append(
                        self._quarantine(
                            batch_id, pos, raw, conflict_problems, REASON_CONFLICT
                        )
                    )
                    return
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

        effect = _EFFECT_HOOKS.get(kind)
        if effect is None:
            # 无外部副作用的事件：一事务落账。
            self._commit_ledger(batch_id, pos, raw, event_id, result)
            return

        effect_name, hook_name = effect

        # 阶段 1：持久化副作用认领（INSERT OR IGNORE + rowcount 判断是否新认领）。
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    "INSERT OR IGNORE INTO side_effect_claims(event_id, effect) VALUES (?, ?)",
                    (event_id, effect_name),
                )
                claim_created = cursor.rowcount > 0
                self._set_status(batch_id, pos, STATUS_DELIVERING, event_id)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

        # 阶段 2：执行外部副作用（事务外）。认领已存在（重提对账场景）时跳过。
        # hook 抛错视同“副作用结果不明”：不重试、不重放，转 delivery 隔离交对账，
        # 同批后续记录继续处理。
        if claim_created:
            try:
                getattr(self._side_effects, hook_name)(event_id, raw)
            except Exception as exc:
                # 注意只捕获 Exception：SystemExit / KeyboardInterrupt 视为真实进程中断，
                # 恢复时由 delivering 状态走同一套 delivery 隔离逻辑。
                result.quarantined.append(
                    self._quarantine(
                        batch_id,
                        pos,
                        raw,
                        [
                            Problem(
                                field=None,
                                code="delivery_failed",
                                message=(
                                    f"副作用 {effect_name} 执行报错（{type(exc).__name__}: {exc}），"
                                    "为避免重复核销/重复结案，记录暂停落账，请对账后凭票据重新提交"
                                ),
                            )
                        ],
                        REASON_DELIVERY,
                    )
                )
                return

        # 阶段 3：确认落账。此处崩溃的话，resume 看到 delivering+认领 → 转隔离，
        # 运营对账确认副作用已发生后重提，因认领已存在不会再次触发。
        self._commit_ledger(batch_id, pos, raw, event_id, result)

    def _commit_ledger(
        self,
        batch_id: str,
        pos: int,
        raw: Any,
        event_id: str,
        result: BatchResult,
    ) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO ledger(event_id, batch_id, position, raw_json, committed_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        event_id,
                        batch_id,
                        pos,
                        json.dumps(raw, ensure_ascii=False),
                        _now(),
                    ),
                )
                self._set_status(batch_id, pos, STATUS_COMMITTED, event_id)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        result.committed.append(event_id)

    def _set_status(
        self, batch_id: str, pos: int, status: str, event_id: str | None
    ) -> None:
        self._conn.execute(
            "UPDATE records SET status=?, event_id=?, updated_at=? "
            "WHERE batch_id=? AND position=?",
            (status, event_id, _now(), batch_id, pos),
        )

    # -- 隔离区 ----------------------------------------------------------

    def _insert_quarantine_row(
        self,
        batch_id: str,
        pos: int,
        event_id: str | None,
        raw: Any,
        problems: Sequence[Problem],
        reason: str,
    ) -> str:
        ticket_id = uuid.uuid4().hex
        self._conn.execute(
            "INSERT INTO quarantine(ticket_id, original_batch_id, original_position, "
            "event_id, reason, raw_json, problems_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ticket_id,
                batch_id,
                pos,
                event_id,
                reason,
                json.dumps(raw, ensure_ascii=False),
                json.dumps(
                    [
                        {"field": p.field, "code": p.code, "message": p.message}
                        for p in problems
                    ],
                    ensure_ascii=False,
                ),
                _now(),
            ),
        )
        return ticket_id

    def _quarantine(
        self,
        batch_id: str,
        pos: int,
        raw: Any,
        problems: Sequence[Problem],
        reason: str,
    ) -> QuarantineTicket:
        """把一条记录放入隔离区（独立事务，绝不影响其他记录）。"""
        event_id = (
            raw.get("event_id")
            if isinstance(raw, dict) and isinstance(raw.get("event_id"), str)
            else None
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                ticket_id = self._insert_quarantine_row(
                    batch_id, pos, event_id, raw, problems, reason
                )
                self._set_status(batch_id, pos, STATUS_QUARANTINED, event_id)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
            row = self._conn.execute(
                "SELECT * FROM quarantine WHERE ticket_id=?", (ticket_id,)
            ).fetchone()
        return self._row_to_ticket(row)

    def list_quarantine(self, batch_id: str | None = None) -> list[QuarantineTicket]:
        """列出未解决的隔离票据（可按原批次过滤）。"""
        with self._lock:
            if batch_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM quarantine WHERE resolved_at IS NULL ORDER BY created_at"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM quarantine WHERE resolved_at IS NULL "
                    "AND original_batch_id=? ORDER BY original_position",
                    (batch_id,),
                ).fetchall()
        return [self._row_to_ticket(r) for r in rows]

    def requeue(
        self,
        ticket_id: str,
        corrected: Any,
        target_batch_id: str | None = None,
    ) -> BatchResult:
        """修正隔离记录后重新提交。

        重新提交带着票据上的**原始批次与原始位置**：默认投回原批次原位置；
        给出 ``target_batch_id`` 时进入该批次，但位置仍记录为原始位置。
        若修正后仍未通过校验，会生成新的隔离票据，旧票据标记为已解决。
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM quarantine WHERE ticket_id=?", (ticket_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"未知隔离票据 {ticket_id!r}")
            ticket = self._row_to_ticket(row)
            target = target_batch_id or ticket.original_batch_id
            now = _now()

            self._conn.execute(
                "INSERT OR IGNORE INTO batches(batch_id, created_at, updated_at, last_pos) "
                "VALUES (?, ?, ?, ?)",
                (target, now, now, -1),
            )
            if target == ticket.original_batch_id:
                # 投回原批次：沿用原始位置（该位置上正是被隔离的坏记录）。
                position = ticket.original_position
            else:
                # 进入其他批次：追加到末尾，避免覆盖目标批次记录；
                # 原始位置仍保留在隔离票据的溯源信息中。
                rowmax = self._conn.execute(
                    "SELECT COALESCE(MAX(position), -1) AS m FROM records WHERE batch_id=?",
                    (target,),
                ).fetchone()
                position = rowmax["m"] + 1
            self._insert_record_row(target, position, corrected, now)
            self._conn.execute(
                "UPDATE quarantine SET resolved_at=? WHERE ticket_id=?",
                (now, ticket_id),
            )
            self._conn.commit()
            return self.resume(target)

    # -- 查询 ------------------------------------------------------------

    def ledger(self) -> list[dict[str, Any]]:
        """已落账事件，按（批次、位置）顺序返回。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, batch_id, position, raw_json, committed_at "
                "FROM ledger ORDER BY batch_id, position"
            ).fetchall()
        return [
            {
                "event_id": r["event_id"],
                "batch_id": r["batch_id"],
                "position": r["position"],
                "record": json.loads(r["raw_json"]),
                "committed_at": r["committed_at"],
            }
            for r in rows
        ]

    def _touch_batch(self, batch_id: str) -> None:
        last = self._conn.execute(
            "SELECT COALESCE(MAX(position), -1) AS m FROM records "
            "WHERE batch_id=? AND status != ?",
            (batch_id, STATUS_PENDING),
        ).fetchone()["m"]
        self._conn.execute(
            "UPDATE batches SET updated_at=?, last_pos=? WHERE batch_id=?",
            (_now(), last, batch_id),
        )
        self._conn.commit()
