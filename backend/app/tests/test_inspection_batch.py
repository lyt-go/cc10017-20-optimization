"""巡检批次表测试：窗口去重、续做边界、中断恢复与死信不重复增长。"""
from __future__ import annotations

import unittest

from app.services.stream_quality import (
    BATCH_TABLE,
    StreamQualityService,
    window_boundary_for,
    window_key_for,
)
from app.services.topic_registry import TopicRegistryService
from app.services.watermark_track import WatermarkTrackService
from app.store import store


class TestWindowBoundary(unittest.TestCase):
    """检测窗口边界与去重键。"""

    def test_boundary_floors_to_window(self) -> None:
        self.assertEqual(window_boundary_for("2026-11-16T10:07:30", "5min"), "2026-11-16T10:05:00")
        self.assertEqual(window_boundary_for("2026-11-16T10:07:30", "10min"), "2026-11-16T10:00:00")
        self.assertEqual(window_boundary_for("2026-11-16T10:07:30", "1h"), "2026-11-16T10:00:00")

    def test_window_key_stable_for_same_late_window(self) -> None:
        key_a = window_key_for("TOPI-0001", "5min", "2026-11-16T10:06:00")
        key_b = window_key_for("TOPI-0001", "5min", "2026-11-16T10:09:59")
        self.assertEqual(key_a, key_b)


class InspectionBatchCase(unittest.TestCase):
    """造数工具：每个用例用独立主题，避免共享窗口键。"""

    def setUp(self) -> None:
        self.quality_svc = StreamQualityService()
        self.wm_svc = WatermarkTrackService()
        self.topic_svc = TopicRegistryService()
        self._seq = 0

    def make_rule(self, *, late_count: int, threshold: int, window: str = "5min") -> dict:
        """登记一套 主题→水位→质量规则，返回质量规则记录。"""
        self._seq += 1
        suffix = f"B{self._seq:03d}{len(store.rows('topic_registry')):03d}"
        topic_code = f"TOPI-{suffix}"
        self.topic_svc.create_entry({"主题编号": topic_code, "主题名称": f"批量测试{suffix}", "消息格式": "JSON"})
        self.wm_svc.create_entry({
            "标记编号": f"WATE-{suffix}", "数据主题": topic_code, "分区编号": "0",
            "事件时间水位": "2026-11-17T10:00:00", "处理时间水位": "2026-11-17T10:02:00",
            "延迟记录数": str(late_count), "消费偏移": "100", "检查时间": "2026-11-17T10:07:00",
        })
        rule, missing = self.quality_svc.create_entry({
            "规则编号": f"STRE-{suffix}", "监控主题": topic_code, "校验类型": "延迟校验",
            "告警阈值": str(threshold), "采样比例": "1.0", "检测窗口": window,
            "阻断动作": "告警并阻断", "规则版本": "v9",
        })
        self.assertEqual(missing, [])
        return rule

    def dead_letters_for(self, topic_code: str) -> list[dict]:
        return [r for r in store.rows("dead_letter") if r.get("原始主题") == topic_code]


class TestBatchInspection(InspectionBatchCase):
    """批量巡检生成巡检批次表：窗口去重结果 + 续做边界。"""

    def test_batch_writes_batch_table(self) -> None:
        rule_ok = self.make_rule(late_count=3, threshold=50)
        rule_bad = self.make_rule(late_count=80, threshold=50)

        batch, message = self.quality_svc.run_batch_inspection([rule_ok["id"], rule_bad["id"]])

        self.assertIsNotNone(batch, message)
        self.assertEqual(batch["status"], "已完成")
        self.assertEqual(batch["去重口径"], "检测窗口")
        self.assertEqual(batch["窗口总数"], 2)
        self.assertEqual(batch["已检查数"], 2)
        self.assertEqual(batch["已去重数"], 0)
        # 续做边界推进到末尾
        self.assertEqual(batch["续做边界"], 2)
        # 来源版本保留在批次表里
        self.assertIn("v9", batch["来源版本"])
        # 每条结果单独列出
        self.assertEqual(len(batch["results"]), 2)
        # 违规窗口只阻断一次：对应主题恰好一条死信，且带窗口键
        dead = self.dead_letters_for(rule_bad["监控主题"])
        self.assertEqual(len(dead), 1)
        expected_key = window_key_for(rule_bad["监控主题"], "5min", "2026-11-17T10:07:00")
        self.assertEqual(dead[0].get("检测窗口键"), expected_key)
        self.assertEqual(dead[0].get("来源批次"), batch["批次编号"])
        # 批次表已落库
        self.assertIsNotNone(self.quality_svc.get_batch(batch["id"]))

    def test_adjacent_batch_dedups_same_window(self) -> None:
        rule = self.make_rule(late_count=80, threshold=50)
        first, _ = self.quality_svc.run_batch_inspection([rule["id"]])
        self.assertEqual(first["已检查数"], 1)
        dead_before = len(self.dead_letters_for(rule["监控主题"]))

        # 同一迟到窗口在相邻批次再次到达：按检测窗口去重
        second, _ = self.quality_svc.run_batch_inspection([rule["id"]])

        self.assertEqual(second["已检查数"], 0)
        self.assertEqual(second["已去重数"], 1)
        self.assertEqual(second["windows"][0]["status"], "已去重")
        self.assertEqual(second["windows"][0]["dedup_of"], first["批次编号"])
        # 每个窗口只有一条巡检结果：死信不重复增长
        self.assertEqual(len(self.dead_letters_for(rule["监控主题"])), dead_before)
        checked = [w for b in store.rows(BATCH_TABLE) for w in b["windows"]
                   if w["status"] == "已检查" and w["topic"] == rule["监控主题"]]
        self.assertEqual(len(checked), 1)

    def test_interrupt_then_resume_keeps_results(self) -> None:
        rule_a = self.make_rule(late_count=90, threshold=50)
        rule_b = self.make_rule(late_count=5, threshold=50)
        total_dead_before = len(store.rows("dead_letter"))

        # 模拟写入中断：只处理第一个窗口
        batch, _ = self.quality_svc.run_batch_inspection([rule_a["id"], rule_b["id"]], fail_after=1)
        self.assertEqual(batch["status"], "已中断")
        self.assertEqual(batch["续做边界"], 1)
        self.assertEqual(len(batch["results"]), 1)
        first_result = dict(batch["results"][0])
        # 中断前已处置的窗口已生成死信
        self.assertEqual(len(store.rows("dead_letter")), total_dead_before + 1)

        # 从断点恢复：只续做未完成窗口，此前完整结果保留
        resumed, message = self.quality_svc.resume_batch(batch["id"])
        self.assertIsNotNone(resumed, message)
        self.assertEqual(resumed["status"], "已完成")
        self.assertEqual(resumed["续做边界"], 2)
        self.assertEqual(len(resumed["results"]), 2)
        # 第一条结果是中断前写下的那一条（未被重扫覆盖）
        self.assertEqual(resumed["results"][0]["checked_at"], first_result["checked_at"])
        self.assertEqual(resumed["results"][0]["window_key"], first_result["window_key"])
        # 第二个窗口通过，不再新增死信；全量重扫会再次阻断的窗口没有重复阻断
        self.assertEqual(len(store.rows("dead_letter")), total_dead_before + 1)
        self.assertEqual(len(self.dead_letters_for(rule_a["监控主题"])), 1)

        # 已完成批次不允许续做
        again, msg = self.quality_svc.resume_batch(batch["id"])
        self.assertIsNone(again)
        self.assertIn("无需续做", msg)

    def test_resume_after_adjacent_batch_conflict(self) -> None:
        """中断批次的待检查窗口被相邻批次检查后，恢复时按冲突判定去重。"""
        rule = self.make_rule(late_count=70, threshold=50)
        # 批次 A 一个窗口都没处理就中断
        batch_a, _ = self.quality_svc.run_batch_inspection([rule["id"]], fail_after=0)
        self.assertEqual(batch_a["status"], "已中断")
        self.assertEqual(batch_a["续做边界"], 0)
        # 相邻批次 B 检查了同一窗口
        batch_b, _ = self.quality_svc.run_batch_inspection([rule["id"]])
        self.assertEqual(batch_b["已检查数"], 1)
        dead_before = len(self.dead_letters_for(rule["监控主题"]))

        # 恢复批次 A：窗口已被 B 检查，按窗口边界去重而不是重复阻断
        resumed, _ = self.quality_svc.resume_batch(batch_a["id"])
        self.assertEqual(resumed["status"], "已完成")
        self.assertEqual(resumed["已检查数"], 0)
        self.assertEqual(resumed["已去重数"], 1)
        self.assertEqual(resumed["windows"][0]["dedup_of"], batch_b["批次编号"])
        self.assertEqual(len(self.dead_letters_for(rule["监控主题"])), dead_before)


class TestWatermarkUncheckedWindows(InspectionBatchCase):
    """水位追踪跨模块使用巡检批次表核对尚未检查的水位窗口。"""

    def test_unchecked_windows_shrink_after_batch(self) -> None:
        rule = self.make_rule(late_count=10, threshold=50)
        before = self.wm_svc.unchecked_windows()
        self.assertTrue(any(w["数据主题"] == rule["监控主题"] for w in before["unchecked"]))
        checked_before = before["checked_count"]

        self.quality_svc.run_batch_inspection([rule["id"]])

        after = self.wm_svc.unchecked_windows()
        self.assertFalse(any(w["数据主题"] == rule["监控主题"] for w in after["unchecked"]))
        self.assertEqual(after["checked_count"], checked_before + 1)
        # 批次来源版本随批次表跨模块传递给水位追踪
        self.assertEqual(after["source_table"], BATCH_TABLE)
        self.assertTrue(any(b["来源版本"] for b in after["source_batches"]))


if __name__ == "__main__":
    unittest.main()
