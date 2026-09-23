"""赛事消费联防事件校验。

第三方补传通道会把任意 JSON 值（null、数组、字符串……）混进批次，
校验器必须对 *任何* 输入返回结构化问题列表而不是抛出异常，
以便上层把坏记录稳定送入隔离区。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

# 事件名称属于对外契约，只允许追加，不允许改动既有取值。
EVENT_KINDS = [
    "EVENT_PUBLISHED",
    "MERCHANT_COMMITMENT",
    "BENEFIT_REDEEMED",
    "COMPLAINT_OPENED",
    "REMEDY_SETTLED",
]
REQUIRED_FIELDS = ("event_id", "kind", "occurred_at", "subject_id", "payload")

# 各类事件允许出现的载荷字段形状。None 表示载荷必须是对象，字段不做限制。
# 受约束的载荷多给/少给字段都会被分别定位到 payload。
PAYLOAD_SCHEMAS: dict[str, set[str] | None] = {
    "BENEFIT_REDEEMED": {"benefit_id", "quota"},
    "COMPLAINT_OPENED": {"complaint_id", "reason"},
    "REMEDY_SETTLED": {"remedy_id", "amount"},
}

_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_:.\-]*$")
_MAX_ID_LENGTH = 128


@dataclass(frozen=True)
class Problem:
    """单条结构化校验问题。

    field 为问题定位（顶层字段名；整条记录形状错误时为 ``"<record>"``），
    code 为机器可读的问题代码，message 为面向运营的说明。
    """

    field: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"field": self.field, "code": self.code, "message": self.message}


def _problem(field: str, code: str, message: str) -> Problem:
    return Problem(field=field, code=code, message=message)


def _valid_identifier(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= _MAX_ID_LENGTH
        and bool(_ID_PATTERN.match(value))
    )


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    text = value.strip()
    # datetime.fromisoformat 接受裸日期，事件时间必须带时间分量。
    if len(text) < 11 or text[10] != "T":
        return False
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _payload_problems(kind: str, payload: Any) -> list[Problem]:
    if not isinstance(payload, dict):
        return [
            _problem(
                "payload",
                "bad_payload",
                f"payload 必须是对象，实际收到 {type(payload).__name__}",
            )
        ]
    allowed = PAYLOAD_SCHEMAS.get(kind)
    if allowed is None:
        return []
    problems: list[Problem] = []
    missing = sorted(allowed - payload.keys())
    if missing:
        problems.append(
            _problem(
                "payload",
                "bad_payload",
                f"{kind} 载荷缺少字段: {', '.join(missing)}",
            )
        )
    unexpected = sorted(set(payload.keys()) - allowed)
    if unexpected:
        problems.append(
            _problem(
                "payload",
                "bad_payload",
                f"{kind} 载荷出现未声明字段: {', '.join(unexpected)}",
            )
        )
    return problems


def validate_event(record: Any) -> list[Problem]:
    """校验单条事件，永远返回问题列表而不抛出异常。

    问题按字段分别定位：缺失字段（missing_field）、未知事件类型
    （unknown_kind）、时间格式（bad_timestamp）、主体标识
    （bad_subject）、载荷形状（bad_payload），以及整条记录不是对象
    （bad_shape）。
    """
    if not isinstance(record, dict):
        return [
            _problem(
                "<record>",
                "bad_shape",
                f"事件记录必须是 JSON 对象，实际收到 {type(record).__name__}",
            )
        ]

    problems: list[Problem] = []

    for name in REQUIRED_FIELDS:
        if name not in record:
            problems.append(_problem(name, "missing_field", f"缺少必填字段 {name}"))

    kind = record.get("kind")
    if "kind" in record:
        if not isinstance(kind, str):
            problems.append(
                _problem(
                    "kind",
                    "unknown_kind",
                    f"kind 必须是字符串，实际收到 {type(kind).__name__}",
                )
            )
        elif kind not in EVENT_KINDS:
            problems.append(
                _problem("kind", "unknown_kind", f"未知事件类型: {kind}")
            )

    if "occurred_at" in record and not _valid_timestamp(record.get("occurred_at")):
        problems.append(
            _problem(
                "occurred_at",
                "bad_timestamp",
                "occurred_at 必须是带时区的 ISO 8601 时间字符串",
            )
        )

    if "subject_id" in record and not _valid_identifier(record.get("subject_id")):
        problems.append(
            _problem(
                "subject_id",
                "bad_subject",
                "subject_id 必须是非空字符串标识（字母数字与 _:.-）",
            )
        )

    if "event_id" in record and not _valid_identifier(record.get("event_id")):
        problems.append(
            _problem(
                "event_id",
                "bad_subject",
                "event_id 必须是非空字符串标识（字母数字与 _:.-）",
            )
        )

    if "payload" in record and isinstance(kind, str) and kind in EVENT_KINDS:
        problems.extend(_payload_problems(kind, record.get("payload")))
    elif "payload" in record:
        # kind 本身不可信时仍需保证载荷形状能被独立定位。
        problems.extend(_payload_problems("", record.get("payload")))

    return problems
