"""水位追踪业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：跨模块接收流式质量的巡检批次表（quality_inspection_batches），
据此取得尚未检查的水位窗口；批次表由流式质量保留来源版本（规则版本）。
"""
from __future__ import annotations

from typing import Any

from app.services.stream_quality import (
    BATCH_TABLE,
    WINDOW_STATUS_CHECKED,
    window_boundary_for,
    window_key_for,
)
from app.store import store

MODULE = "watermark_track"
LIST_FIELDS = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
REQUIRED_FIELDS = ["标记编号", "数据主题", "分区编号"]
STATUS_ORDER = ["草稿", "正常", "滞后", "已停用"]
ACTION_RULES = {"标记正常": "正常", "标记滞后": "滞后", "停用追踪": "已停用"}
NEGATIVE_ACTIONS = ["停用追踪"]


class WatermarkTrackService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("标记编号", "")).casefold()]
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
            return None, f"水位标记 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于水位追踪可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"水位标记已{action}"

    def unchecked_windows(self) -> dict[str, Any]:
        """用巡检批次表核对水位窗口：返回尚未检查的水位窗口清单。

        跨模块接收流式质量的巡检批次表；只有批次表里「已检查」的窗口键
        才视为已核对，待检查/已去重都不算完成，仍属于尚未检查集合。
        """
        batches = store.rows(BATCH_TABLE)
        checked_keys = {
            window.get("window_key")
            for batch in batches
            for window in batch.get("windows", [])
            if window.get("status") == WINDOW_STATUS_CHECKED
        }

        unchecked: list[dict[str, Any]] = []
        for wm in store.rows(MODULE):
            if wm.get("status") == "已停用":
                continue
            topic = wm.get("数据主题")
            rule = self._find_quality_rule(topic)
            spec = str(rule.get("检测窗口")) if rule and rule.get("检测窗口") else "5min"
            key = window_key_for(topic, spec, wm.get("检查时间"))
            if key in checked_keys:
                continue
            unchecked.append({
                "标记编号": wm.get("标记编号"),
                "数据主题": topic,
                "分区编号": wm.get("分区编号"),
                "检测窗口": spec,
                "窗口边界": window_boundary_for(wm.get("检查时间"), spec),
                "延迟记录数": wm.get("延迟记录数"),
                "水位状态": wm.get("status"),
                "规则版本": rule.get("规则版本") if rule else "-",
            })

        return {
            "source_table": BATCH_TABLE,
            "unchecked": unchecked,
            "unchecked_count": len(unchecked),
            "checked_count": len(checked_keys),
            "source_batches": [
                {
                    "批次编号": batch.get("批次编号"),
                    "来源版本": batch.get("来源版本"),
                    "去重口径": batch.get("去重口径"),
                    "status": batch.get("status"),
                }
                for batch in sorted(batches, key=lambda b: int(b.get("id", 0)), reverse=True)
            ],
        }

    def _find_quality_rule(self, topic_code: str | None) -> dict[str, Any] | None:
        """监控该主题的流式质量规则：优先已生效，其次任意状态。"""
        fallback = None
        for row in store.rows("stream_quality"):
            if row.get("监控主题") != topic_code:
                continue
            if row.get("status") == "已生效":
                return row
            if fallback is None:
                fallback = row
        return fallback
