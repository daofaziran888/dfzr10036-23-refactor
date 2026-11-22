"""延迟监控服务：采集延迟数据后级联告警和健康评估。

延迟来源（维表缓存失效 / 监控采样）统一归集到 lag_source 的延迟来源明细，
本服务只负责登记监控采样来源，并消费来源明细生成实时延迟告警和健康评估。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.lag_source import (
    SOURCE_SAMPLING,
    STEP_ALERT,
    STEP_HEALTH,
    lag_source_service,
)
from app.store import store


class LagMonitorService(BaseModuleService):
    MODULE = "lag_monitor"
    ENTITY = "延迟指标"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["指标编号", "数据流", "源端位点", "处理位点", "延迟秒数", "告警阈值", "最近采样", "采样间隔"]
    REQUIRED_FIELDS = ["指标编号", "数据流", "源端位点"]
    STATUS_ORDER = ["采集中", "正常", "延迟偏高", "严重滞后"]
    ACTION_RULES = {"启动采集": "正常", "标记偏高": "延迟偏高", "标记滞后": "严重滞后"}
    NEGATIVE_ACTIONS = []

    def compute_lag_from_watermark_and_checkpoint(self, data_stream: str) -> dict[str, Any]:
        """从watermark_mgr和checkpoint中读取位点数据计算延迟。

        数据流链路: watermark_mgr + checkpoint -> lag_monitor
        """
        result: dict[str, Any] = {
            "data_stream": data_stream,
            "watermark": None,
            "checkpoint": None,
            "lag_seconds": 0,
            "status_override": None,
        }

        for wm in store.rows("watermark_mgr"):
            if str(wm.get("数据流", "")) == data_stream:
                result["watermark"] = wm.get("当前位点")
                if wm.get("status") == "水位延迟":
                    result["lag_seconds"] = int(wm.get("延迟行数", 0))
                    result["status_override"] = "严重滞后"
                break

        for cp in store.rows("checkpoint"):
            if str(cp.get("数据流", "")) == data_stream:
                result["checkpoint"] = cp.get("保存位点")
                result["save_time"] = cp.get("保存时间")
                break

        return result

    def record_sampling_observation(
        self, entry_id: int, *, lag_status: str | None = None
    ) -> dict[str, Any] | None:
        """监控采样报出新的延迟状态（如独立严重滞后）时，登记一笔监控采样来源观测。

        采样观测与缓存失效观测在共用归集中各自独立、同数据流互不覆盖；
        来源定位到触发时的采样指标和位点。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        if lag_status is not None and entry.get("status") != lag_status:
            entry["status"] = lag_status
            entry["pending"] = True
            entry["abnormal"] = lag_status in ("延迟偏高", "严重滞后")

        return lag_source_service.register_observation(
            source_type=SOURCE_SAMPLING,
            data_stream=str(entry.get("数据流", "")),
            lag_seconds=entry.get("延迟秒数"),
            lag_status=str(entry.get("status")),
            threshold=str(entry.get("告警阈值", "30s")),
            sample_time=str(entry.get("最近采样", "")),
            lag_row_id=entry_id,
            source_ref_id=entry_id,
            source_ref_code=str(entry.get("指标编号", "")),
            source_basis={
                "模块": self.MODULE,
                "指标编号": entry.get("指标编号"),
                "数据流": entry.get("数据流"),
                "源端位点": entry.get("源端位点"),
                "处理位点": entry.get("处理位点"),
                "延迟秒数": entry.get("延迟秒数"),
                "告警阈值": entry.get("告警阈值"),
                "最近采样": entry.get("最近采样"),
                "采样间隔": entry.get("采样间隔"),
            },
        )

    def _ensure_observation(self, entry: dict[str, Any]) -> dict[str, Any]:
        """取指标对应的来源观测；旧入口没有观测时按当前采样补登记一笔。"""
        obs = lag_source_service.find_for_lag_row(int(entry.get("id", 0)))
        if obs is None:
            obs = self.record_sampling_observation(int(entry.get("id", 0)))
        return obs

    def _legacy_health(self, entry: dict[str, Any]) -> dict[str, Any]:
        """正常采样观测无待处置步骤时，沿用旧口径补一份健康评估，保持返回字段不变。"""
        health_rows = store.rows("stream_health")
        new_health_id = max((int(r.get("id", 0)) for r in health_rows), default=0) + 1
        health = {
            "id": new_health_id,
            "评估编号": f"STRE-{new_health_id:04d}",
            "数据流": str(entry.get("数据流", "")),
            "健康分数": 72,
            "风险项": "高延迟+水位堆积",
            "修复建议": "扩容处理节点+扩大乱序窗口",
            "评估时间": "2026-11-18 10:40:00",
            "下次评估": "2026-11-18 10:45:00",
            "评估状态": "待修复",
            "status": "需修复",
            "pending": True,
            "abnormal": True,
        }
        health_rows.append(health)
        return health

    def escalate_to_alert_and_health(self, entry_id: int) -> dict[str, Any] | None:
        """延迟达到告警阈值后，消费延迟来源明细触发告警规则和健康评估。

        数据流链路: lag_source(延迟来源明细) -> alert_rule + stream_health
        告警条件、级别与健康评分沿用原口径；处置按观测分步推进、幂等可续办，
        已生成的告警和评估始终指向当时登记的来源观测。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        obs = self._ensure_observation(entry)
        results: dict[str, Any] = {"alert": None, "health": None}

        if not lag_source_service.pending_steps(obs):
            # 无待处置步骤（正常采样，或该观测已处置完成）：不重复产出。
            # 正常指标的旧入口口径仍会返回一份健康评估。
            if entry.get("status") not in ("严重滞后", "延迟偏高"):
                results["health"] = self._legacy_health(entry)
            return results

        # 消费共用延迟来源明细：逐步执行该观测剩余处置，中断后续办不会重复写。
        while True:
            step_result = lag_source_service.run_next_step(int(obs["id"]))
            if step_result is None:
                break
            if step_result["step"] == STEP_ALERT:
                results["alert"] = step_result["output"]
            elif step_result["step"] == STEP_HEALTH:
                results["health"] = step_result["output"]

        return results
