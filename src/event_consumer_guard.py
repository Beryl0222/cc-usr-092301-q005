"""赛事消费联防事件的领域约定与健壮校验。

第三方补传的批次里可能混入任何 JSON 值（``None``、数组、字符串、数字），
校验器必须对任意输入返回结构化问题，而不是在调用字典方法时抛错。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

EVENT_KINDS = [
    "EVENT_PUBLISHED",
    "MERCHANT_COMMITMENT",
    "BENEFIT_REDEEMED",
    "COMPLAINT_OPENED",
    "REMEDY_SETTLED",
]
REQUIRED_FIELDS = ("event_id", "kind", "occurred_at", "subject_id", "payload")

# 对每种事件载荷形状的领域约定：元组内为该类型必须存在的载荷字段。
# EVENT_PUBLISHED 的载荷为自由形状（基线样例仅含 note），只要求是 JSON 对象。
PAYLOAD_SCHEMAS: dict[str, tuple[str, ...]] = {
    "EVENT_PUBLISHED": (),
    "MERCHANT_COMMITMENT": ("merchant_id",),
    "BENEFIT_REDEEMED": ("benefit_id", "redeem_amount"),
    "COMPLAINT_OPENED": ("complaint_id",),
    "REMEDY_SETTLED": ("complaint_id", "settle_amount"),
}

# RFC 3339 风格：日期T时间 + 可选的偏移量（Z 或 ±HH:MM），秒可省略。
_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"
)


@dataclass(frozen=True)
class Problem:
    """单条记录的一个定位问题。

    ``field`` 为 ``None`` 表示整条记录形状错误（不是 JSON 对象）；
    ``code`` 供程序判断，``message`` 供运营解释。
    """

    field: str | None
    code: str
    message: str


@dataclass
class ValidationReport:
    """对单条原始值的校验结论。"""

    valid: bool
    problems: list[Problem] = field(default_factory=list)

    def problem_codes(self) -> list[str]:
        return [p.code for p in self.problems]

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "problems": [
                {"field": p.field, "code": p.code, "message": p.message}
                for p in self.problems
            ],
        }


def _is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_plain_object(value: Any) -> bool:
    return isinstance(value, dict)


def _parse_timestamp(value: Any) -> datetime | None:
    """解析 RFC 3339 时间；``Z`` 结尾和空格分隔也接受，非法返回 None。"""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not _RFC3339_RE.match(text):
        return None
    normalized = text.replace("Z", "+00:00")
    if " " in normalized:
        normalized = normalized.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def validate_event(record: Any) -> list[str]:
    """兼容基线的字段名问题列表。

    对任意 JSON 值都不会抛错：整条形状错误时返回 ``["__root__"]``；
    字段问题用字段名标记，``kind`` 问题统一记为 ``"kind"``。
    """
    return [p.field or "__root__" for p in validate_event_detailed(record).problems]


def validate_event_detailed(record: Any) -> ValidationReport:
    """对任意 JSON 值返回结构化校验结论。"""
    problems: list[Problem] = []

    if not _is_plain_object(record):
        actual = type(record).__name__ if record is not None else "null"
        return ValidationReport(
            valid=False,
            problems=[
                Problem(
                    field=None,
                    code="not_an_object",
                    message=f"事件记录必须是 JSON 对象，实际收到 {actual}",
                )
            ],
        )

    # 1. 字段缺失
    for name in REQUIRED_FIELDS:
        if name not in record:
            problems.append(Problem(name, "missing_field", f"缺少必填字段 {name}"))

    # event_id
    if "event_id" in record and not _is_nonempty_string(record["event_id"]):
        problems.append(
            Problem("event_id", "invalid_identifier", "event_id 必须是非空字符串")
        )

    # kind
    kind = record.get("kind")
    if "kind" in record:
        if not isinstance(kind, str):
            problems.append(Problem("kind", "invalid_type", "kind 必须是字符串"))
        elif kind not in EVENT_KINDS:
            problems.append(
                Problem(
                    "kind",
                    "unknown_kind",
                    f"未知事件类型 {kind!r}，允许：{', '.join(EVENT_KINDS)}",
                )
            )

    # occurred_at
    if "occurred_at" in record and _parse_timestamp(record["occurred_at"]) is None:
        problems.append(
            Problem(
                "occurred_at",
                "invalid_timestamp",
                "occurred_at 必须是 RFC 3339 时间（如 2026-09-20T09:00:00+08:00）",
            )
        )

    # subject_id
    if "subject_id" in record and not _is_nonempty_string(record["subject_id"]):
        problems.append(
            Problem("subject_id", "invalid_subject", "subject_id 必须是非空字符串")
        )

    # payload
    if "payload" in record:
        payload = record["payload"]
        if not isinstance(payload, dict):
            problems.append(
                Problem("payload", "invalid_payload_shape", "payload 必须是 JSON 对象")
            )
        else:
            schema = PAYLOAD_SCHEMAS.get(kind) if isinstance(kind, str) else None
            if schema:
                for key in schema:
                    if key not in payload:
                        problems.append(
                            Problem(
                                f"payload.{key}",
                                "missing_payload_field",
                                f"{kind} 载荷缺少字段 {key}",
                            )
                        )

    return ValidationReport(valid=not problems, problems=problems)
