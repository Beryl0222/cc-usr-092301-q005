# 赛事消费异常联防

赛事消费事件的批量接收服务。第三方补传通道会把任意 JSON 值混入批次，
本服务保证坏记录被结构化隔离、好记录可靠落账，并在进程中断后可恢复、
副作用不重复触发。

## 目录

- `src/event_consumer_guard.py`：事件名称与字段校验。`validate_event`
  接受任意 JSON 值（含 null、数组、字符串），永不抛错，返回结构化
  `Problem(field, code, message)` 列表，分别定位：
  - `bad_shape`：整条记录不是 JSON 对象；
  - `missing_field`：缺少 `event_id` / `kind` / `occurred_at` / `subject_id` / `payload`；
  - `unknown_kind`：未知事件类型；
  - `bad_timestamp`：时间不是带时间分量的 ISO 8601 字符串（接受 `Z` 后缀）；
  - `bad_subject`：事件/主体标识不是合法非空字符串；
  - `bad_payload`：载荷不是对象或形状不符。
- `src/batch_receiver.py`：可靠批量接收路径 `BatchReceiver`：
  - 坏记录进入隔离区并保留批次号与批次内原始位置；好记录按原顺序落账；
  - 以 event_id 内容指纹幂等：完全重放只确认一次；内容冲突拒绝覆盖并隔离；
  - 权益核销（`BENEFIT_REDEEMED`）与投诉结案（`REMEDY_SETTLED`）等副作用
    先持久化 `reserved` 预留再触发；中断恢复只列入口待人工 `reconcile`，
    绝不自动重放；
  - 隔离记录可用 `resubmit_quarantine(qid, corrected)` 带原始位置重新提交。
- `data/sample.json`：用于核对资料格式的虚构事件。
- `tests/`：校验边界与批量接收（混合批次、冲突重放、崩溃恢复、重新入队）。

## 本地核对

```bash
python3 -m unittest discover -s tests
```
