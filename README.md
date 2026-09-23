# 赛事消费异常联防

赛事消费联防服务的事件领域约定与可靠批量接收组件。第三方补传的批次中可能混入
`null`、数组、字符串等任意 JSON 值，本组件保证：坏记录可定位、可隔离、可修正重提，
好记录不受影响；重复投递有确定结果；进程中断后可安全续跑且不重复触发权益核销、
投诉结案等副作用。资料只包含领域约定与虚构样例，不含真实个人信息或生产连接。

## 目录

- `src/event_consumer_guard.py`：事件种类与健壮校验。
- `src/batch_receiver.py`：持久化批量接收（隔离区、幂等落账、断点续跑、修正重提）。
- `data/sample.json`：用于核对资料格式的虚构事件。
- `tests/`：校验边界与接收路径的可运行测试。

## 校验器：任意 JSON 值都不崩溃

```python
from src.event_consumer_guard import validate_event, validate_event_detailed

validate_event_detailed(None)          # valid=False，问题 code=not_an_object
validate_event_detailed({"kind": "X"}) # 逐字段定位：missing_field / unknown_kind / ...
```

问题结构为 `Problem(field, code, message)`，分别定位：

| code | 含义 |
| --- | --- |
| `not_an_object` | 整条记录不是 JSON 对象（`field=None`） |
| `missing_field` | 缺少必填字段（event_id/kind/occurred_at/subject_id/payload） |
| `unknown_kind` / `invalid_type` | 未知事件类型 / kind 不是字符串 |
| `invalid_timestamp` | occurred_at 不是 RFC 3339 时间 |
| `invalid_subject` / `invalid_identifier` | subject_id / event_id 不是非空字符串 |
| `invalid_payload_shape` / `missing_payload_field` | payload 不是对象 / 载荷缺字段 |

`validate_event(record)` 保留基线签名：返回字段名问题列表（整体形状错误为 `["__root__"]`），
对任意输入都不抛错。事件名称沿用 `EVENT_KINDS`：`EVENT_PUBLISHED`、
`MERCHANT_COMMITMENT`、`BENEFIT_REDEEMED`、`COMPLAINT_OPENED`、`REMEDY_SETTLED`。

## 批量接收语义

```python
from src.batch_receiver import BatchReceiver, SideEffectHandler

class Ops(SideEffectHandler):
    def benefit_redeemed(self, event_id, record): ...   # 权益核销
    def complaint_opened(self, event_id, record): ...   # 投诉立案
    def complaint_closed(self, event_id, record): ...   # 投诉结案/补偿

with BatchReceiver("state.db", side_effects=Ops()) as rx:
    result = rx.ingest(records, batch_id="2026-09-23-补传批次A")
    rx.resume("2026-09-23-补传批次A")  # 中断后新进程续跑
```

- **混合批次**：坏记录进入隔离区（`result.quarantined`，附结构化问题与原始位置），
  好记录按批次内原始顺序落账（`rx.ledger()`）。
- **完全重放**：同一 `event_id` 且内容规范化一致（与键序、空白无关）只确认一次，
  计入 `result.replayed`，不重复执行副作用。
- **内容冲突**：同一 `event_id` 内容不一致时不覆盖旧记录，冲突投递转入隔离区
  （原因 `conflict`），供运营解释与裁决。
- **断点续跑与副作用恰好一次**：副作用采用「持久化认领 → 执行 → 确认落账」两段式。
  进程在副作用执行前后被杀死，恢复时不会第二次核销/结案，记录转入原因 `delivery`
  的隔离区等待对账。
- **修正重提**：`rx.requeue(ticket_id, corrected)` 按票据把修正后的记录投回原批次
  的原始位置；delivery 票据重提时因副作用认领已存在，只补落账、不再次触发副作用。

状态存储为单个 sqlite 文件（默认 `:memory:`），无需外部依赖。

## 本地核对

```bash
python3 -m unittest discover -s tests
```
