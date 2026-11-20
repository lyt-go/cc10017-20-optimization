"""流式质量业务规则：质量规则管理、校验执行与死信联动。

增强：质量规则生效时触发水位检查，异常时生成死信记录。

批量巡检优化：新增巡检批次表（quality_inspection_batches）。
- 批次表同时记录窗口去重结果和续做边界（resume cursor）；
- 冲突判定后按检测窗口边界去重：每个迟到窗口全平台只有一条巡检结果，
  同一窗口在相邻批次再次到达时标记「已去重」，不再重复阻断（不重复生成死信）；
- 写入中断时批次状态置为「已中断」，此前完整结果保留在批次表里，
  恢复巡检只续做未完成窗口，避免全量重扫把已处置窗口再次阻断。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "stream_quality"
LIST_FIELDS = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
REQUIRED_FIELDS = ["规则编号", "监控主题", "校验类型"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]

# 巡检批次表：跨模块共享，水位追踪模块据此取得尚未检查的水位窗口
BATCH_TABLE = "quality_inspection_batches"
# 冲突判定结论：批量巡检按检测窗口边界去重（而非按主题状态去重）
DEDUP_MODE = "检测窗口"
WINDOW_STATUS_PENDING = "待检查"
WINDOW_STATUS_CHECKED = "已检查"
WINDOW_STATUS_DEDUPED = "已去重"
BATCH_STATUS_RUNNING = "进行中"
BATCH_STATUS_INTERRUPTED = "已中断"
BATCH_STATUS_DONE = "已完成"


def _window_minutes(window_spec: str) -> int:
    """把「5min」「1h」这类检测窗口规格解析成分钟数，缺省 5 分钟。"""
    text = str(window_spec or "").strip().lower()
    try:
        if text.endswith("min"):
            return max(1, int(text[:-3]))
        if text.endswith("h"):
            return max(1, int(text[:-1]) * 60)
        if text.endswith("s"):
            return 1
        return max(1, int(text))
    except (ValueError, TypeError):
        return 5


def window_boundary_for(check_time: Any, window_spec: str) -> str:
    """把水位检查时间按检测窗口大小向下取整，得到窗口边界（ISO 时间串）。"""
    try:
        ts = datetime.fromisoformat(str(check_time))
    except (ValueError, TypeError):
        ts = datetime.now()
    minutes = _window_minutes(window_spec)
    floored = ts.replace(second=0, microsecond=0)
    if minutes >= 60:
        hours = minutes // 60
        floored = floored.replace(minute=0, hour=(floored.hour // hours) * hours)
    else:
        floored = floored.replace(minute=(floored.minute // minutes) * minutes)
    return floored.isoformat()


def window_key_for(topic: str, window_spec: str, check_time: Any) -> str:
    """检测窗口去重键：主题 + 窗口规格 + 窗口边界。同一迟到窗口键恒定。"""
    return f"{topic}|{window_spec}|{window_boundary_for(check_time, window_spec)}"


class StreamQualityService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        # 校验监控主题
        topic_code = values.get("监控主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"监控主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"质量规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式质量可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记生效":
            topic_code = entry.get("监控主题")
            watermark = self._find_watermark(topic_code)
            if not watermark:
                return None, f"主题 {topic_code} 未配置水位追踪"
            # 执行质量检查
            self._run_quality_check(entry, watermark)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"质量规则已{action}"

    # ───────────────────────────────
    # 批量巡检：巡检批次表（窗口去重 + 续做边界）
    # ───────────────────────────────

    def list_batches(self) -> list[dict[str, Any]]:
        """巡检批次表列表，新批次在前。"""
        batches = store.rows(BATCH_TABLE)
        return sorted(batches, key=lambda b: int(b.get("id", 0)), reverse=True)

    def get_batch(self, batch_id: int) -> dict[str, Any] | None:
        return store.find(BATCH_TABLE, batch_id)

    def run_batch_inspection(
        self,
        rule_ids: list[int],
        *,
        fail_after: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """一次提交多条质量规则做批量巡检，生成巡检批次表。

        批次表保留窗口去重结果（已去重窗口及其来源批次）和续做边界；
        fail_after 用于模拟写入中断：处理完 N 个窗口后批次置为「已中断」。
        """
        rules: list[dict[str, Any]] = []
        skipped: list[str] = []
        for rule_id in rule_ids:
            try:
                rule = store.find(MODULE, int(rule_id))
            except (ValueError, TypeError):
                rule = None
            if rule is None:
                skipped.append(f"规则 {rule_id} 不存在")
                continue
            watermark = self._find_watermark(rule.get("监控主题"))
            if watermark is None:
                skipped.append(f"{rule.get('规则编号')} 未配置水位追踪")
                continue
            rules.append(rule)
        if not rules:
            detail = "；".join(skipped) if skipped else "未选择质量规则"
            return None, f"没有可巡检的质量规则：{detail}"

        batch = self._create_batch(rules)
        self._process_batch_windows(batch, fail_after=fail_after)

        message = self._batch_message(batch)
        if skipped:
            message += f"；跳过：{'；'.join(skipped)}"
        return batch, message

    def resume_batch(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        """从续做边界恢复中断的批次：只续做未完成窗口，保留此前完整结果。"""
        batch = store.find(BATCH_TABLE, batch_id)
        if batch is None:
            return None, f"巡检批次 {batch_id} 不存在"
        if batch.get("status") == BATCH_STATUS_DONE:
            return None, f"巡检批次 {batch.get('批次编号')} 已完成，无需续做"
        pending = [w for w in batch.get("windows", []) if w.get("status") == WINDOW_STATUS_PENDING]
        if not pending:
            batch["status"] = BATCH_STATUS_DONE
            self._refresh_batch_progress(batch)
            return None, f"巡检批次 {batch.get('批次编号')} 没有待检查窗口"

        batch["status"] = BATCH_STATUS_RUNNING
        self._process_batch_windows(batch, fail_after=None)
        return batch, f"已从断点恢复巡检，{self._batch_message(batch)}"

    def _create_batch(self, rules: list[dict[str, Any]]) -> dict[str, Any]:
        """顺着既有数据流（规则→水位→死信）定位批次表写入位置：在窗口展开时建档。"""
        batches = store.rows(BATCH_TABLE)
        batch_id = max((int(b.get("id", 0)) for b in batches), default=0) + 1
        batch_code = f"INSP-{batch_id:04d}"

        # 冲突判定：按检测窗口边界去重。已检查窗口全平台只保留一条巡检结果，
        # 同一迟到窗口在相邻批次再次到达时直接复用此前结果。
        checked_index = self._checked_window_index()
        seen_in_batch: dict[str, str] = {}
        windows: list[dict[str, Any]] = []
        for rule in rules:
            topic = rule.get("监控主题")
            watermark = self._find_watermark(topic)
            spec = str(rule.get("检测窗口") or "5min")
            key = window_key_for(topic, spec, watermark.get("检查时间"))
            window = {
                "window_key": key,
                "topic": topic,
                "rule_id": rule.get("id"),
                "rule_code": rule.get("规则编号"),
                "规则版本": rule.get("规则版本"),
                "检测窗口": spec,
                "窗口边界": window_boundary_for(watermark.get("检查时间"), spec),
                "延迟记录数": watermark.get("延迟记录数"),
                "告警阈值": rule.get("告警阈值"),
                "status": WINDOW_STATUS_PENDING,
                "result": None,
                "dead_letter_id": None,
                "dedup_of": None,
                "checked_at": None,
            }
            owner = checked_index.get(key)
            if owner is not None:
                # 相邻批次再次到达的同一迟到窗口：去重，不再阻断
                window["status"] = WINDOW_STATUS_DEDUPED
                window["result"] = "去重跳过"
                window["dedup_of"] = owner["批次编号"]
            elif key in seen_in_batch:
                window["status"] = WINDOW_STATUS_DEDUPED
                window["result"] = "去重跳过"
                window["dedup_of"] = batch_code
            else:
                seen_in_batch[key] = batch_code
            windows.append(window)

        now = datetime.now().isoformat()
        versions = sorted({str(rule.get("规则版本") or "-") for rule in rules})
        batch = {
            "id": batch_id,
            "批次编号": batch_code,
            "主题列表": [rule.get("监控主题") for rule in rules],
            "规则列表": [rule.get("规则编号") for rule in rules],
            "来源版本": ",".join(versions),
            "去重口径": DEDUP_MODE,
            "窗口总数": len(windows),
            "已检查数": 0,
            "已去重数": 0,
            "续做边界": 0,
            "status": BATCH_STATUS_RUNNING,
            "创建时间": now,
            "更新时间": now,
            "完成时间": None,
            "windows": windows,
            "results": [],
        }
        self._refresh_batch_progress(batch)
        batches.append(batch)
        return batch

    def _process_batch_windows(self, batch: dict[str, Any], *, fail_after: int | None) -> None:
        """逐窗口巡检：每完成一个窗口就持久化续做边界，中断时保留此前完整结果。"""
        processed_this_run = 0
        for window in batch.get("windows", []):
            if window.get("status") != WINDOW_STATUS_PENDING:
                continue  # 已检查/已去重窗口不重扫，断点续做只处理未完成窗口
            if fail_after is not None and processed_this_run >= fail_after:
                batch["status"] = BATCH_STATUS_INTERRUPTED
                batch["更新时间"] = datetime.now().isoformat()
                return
            # 冲突判定：恢复巡检时该窗口可能已被相邻批次检查过
            owner = self._checked_window_index(exclude_batch_id=batch.get("id")).get(window["window_key"])
            if owner is not None:
                window["status"] = WINDOW_STATUS_DEDUPED
                window["result"] = "去重跳过"
                window["dedup_of"] = owner["批次编号"]
            else:
                self._check_window(batch, window)
                processed_this_run += 1
            self._refresh_batch_progress(batch)
            batch["更新时间"] = datetime.now().isoformat()

        batch["status"] = BATCH_STATUS_DONE
        batch["完成时间"] = datetime.now().isoformat()
        batch["更新时间"] = batch["完成时间"]
        self._refresh_batch_progress(batch)

    def _check_window(self, batch: dict[str, Any], window: dict[str, Any]) -> None:
        """检查单个迟到窗口：违规时阻断（生成死信），结果逐条写入批次表。"""
        rule = store.find(MODULE, int(window.get("rule_id") or 0))
        watermark = self._find_watermark(window.get("topic"))
        try:
            threshold = float(window.get("告警阈值") or 0)
        except (ValueError, TypeError):
            threshold = 0.0
        try:
            late_count = float(watermark.get("延迟记录数", 0)) if watermark else 0.0
        except (ValueError, TypeError):
            late_count = 0.0

        violations: list[dict[str, Any]] = []
        if late_count > threshold:
            violations.append({
                "type": "延迟超限",
                "detail": f"延迟记录 {int(late_count)} 超过阈值 {threshold}",
                "topic": window.get("topic"),
            })
            if rule is not None and watermark is not None:
                dead_letter = self._create_dead_letter(
                    rule, watermark, "延迟超限",
                    window_key=window["window_key"], batch_code=batch.get("批次编号"))
                window["dead_letter_id"] = dead_letter.get("id")

        now = datetime.now().isoformat()
        window["status"] = WINDOW_STATUS_CHECKED
        window["result"] = "告警" if violations else "通过"
        window["checked_at"] = now
        # 每条结果单独列出，中断后仍保留在批次表里
        batch["results"].append({
            "批次编号": batch.get("批次编号"),
            "topic": window.get("topic"),
            "rule_code": window.get("rule_code"),
            "window_key": window.get("window_key"),
            "窗口边界": window.get("窗口边界"),
            "检测窗口": window.get("检测窗口"),
            "result": window["result"],
            "延迟记录数": int(late_count),
            "告警阈值": threshold,
            "violations": violations,
            "dead_letter_id": window.get("dead_letter_id"),
            "checked_at": now,
        })

    def _refresh_batch_progress(self, batch: dict[str, Any]) -> None:
        """刷新窗口去重结果计数与续做边界（第一个待检查窗口的位置）。"""
        windows = batch.get("windows", [])
        batch["已检查数"] = sum(1 for w in windows if w.get("status") == WINDOW_STATUS_CHECKED)
        batch["已去重数"] = sum(1 for w in windows if w.get("status") == WINDOW_STATUS_DEDUPED)
        pending = [i for i, w in enumerate(windows) if w.get("status") == WINDOW_STATUS_PENDING]
        batch["续做边界"] = pending[0] if pending else len(windows)

    def _checked_window_index(self, exclude_batch_id: int | None = None) -> dict[str, dict[str, Any]]:
        """全平台已检查窗口索引：window_key → 所属批次。每个窗口只有一条巡检结果。"""
        index: dict[str, dict[str, Any]] = {}
        for batch in store.rows(BATCH_TABLE):
            if exclude_batch_id is not None and int(batch.get("id", 0)) == exclude_batch_id:
                continue
            for window in batch.get("windows", []):
                if window.get("status") == WINDOW_STATUS_CHECKED:
                    index[window["window_key"]] = {
                        "批次编号": batch.get("批次编号"),
                        "result": window.get("result"),
                    }
        return index

    def _batch_message(self, batch: dict[str, Any]) -> str:
        dead_letters = sum(1 for r in batch.get("results", []) if r.get("dead_letter_id"))
        status_text = {
            BATCH_STATUS_DONE: "巡检完成",
            BATCH_STATUS_INTERRUPTED: "写入中断，可从断点恢复",
            BATCH_STATUS_RUNNING: "巡检进行中",
        }.get(batch.get("status"), batch.get("status", ""))
        return (
            f"批次 {batch.get('批次编号')}{status_text}："
            f"窗口 {batch.get('窗口总数')} 个，已检查 {batch.get('已检查数')}，"
            f"去重 {batch.get('已去重数')}，续做边界 {batch.get('续做边界')}，"
            f"新增死信 {dead_letters} 条"
        )

    # ───────────────────────────────
    # 既有单规则数据流
    # ───────────────────────────────

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_watermark(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("watermark_track"):
            if row.get("数据主题") == topic_code:
                return row
        return None

    def _run_quality_check(self, entry: dict[str, Any], watermark: dict[str, Any]) -> dict[str, Any]:
        """执行质量检查，异常时产生死信记录。"""
        try:
            threshold = float(entry.get("告警阈值", "0"))
        except (ValueError, TypeError):
            threshold = 0.0
        try:
            late_count = float(watermark.get("延迟记录数", "0"))
        except (ValueError, TypeError):
            late_count = 0.0

        violations = []
        if late_count > threshold:
            violations.append({
                "type": "延迟超限",
                "detail": f"延迟记录 {int(late_count)} 超过阈值 {threshold}",
                "topic": entry.get("监控主题"),
            })
            # 产生死信
            self._create_dead_letter(entry, watermark, "延迟超限")

        result = {
            "rule_id": entry.get("id"),
            "rule_code": entry.get("规则编号"),
            "topic": entry.get("监控主题"),
            "threshold": threshold,
            "late_count": late_count,
            "violations": violations,
            "checked_at": datetime.now().isoformat(),
            "passed": len(violations) == 0,
        }
        self._ensure_artifact_table("quality_check_results")
        store.rows("quality_check_results").append(result)
        entry["_last_check"] = result
        return result

    def _create_dead_letter(
        self,
        entry: dict[str, Any],
        watermark: dict[str, Any],
        reason: str,
        *,
        window_key: str | None = None,
        batch_code: str | None = None,
    ) -> dict[str, Any]:
        """生成死信；带窗口键时按窗口去重，同一迟到窗口只阻断一次。"""
        dl_rows = store.rows("dead_letter")
        if window_key:
            for row in dl_rows:
                if row.get("检测窗口键") == window_key:
                    return row
        dl_id = max((int(r.get("id", 0)) for r in dl_rows), default=0) + 1
        dl = {
            "id": dl_id,
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "记录编号": f"DEAD-{dl_id:04d}",
            "原始主题": entry.get("监控主题"),
            "原始分区": watermark.get("分区编号", "0"),
            "失败原因": reason,
            "原始消息体": f"延迟记录数:{watermark.get('延迟记录数')}",
            "重试次数": "0",
            "入库时间": datetime.now().isoformat(),
            "处置状态": "待分析",
        }
        if window_key:
            dl["检测窗口键"] = window_key
        if batch_code:
            dl["来源批次"] = batch_code
        dl_rows.append(dl)
        return dl

    def get_quality_results(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("quality_check_results")
        return [r for r in store.rows("quality_check_results") if r.get("rule_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
