"""延迟来源归集服务：把维表缓存失效和延迟监控采样两类来源归并为共用的延迟来源明细。

设计要点
- 两类来源（缓存失效 / 监控采样）各自登记一条观测，同数据流的多笔观测按登记顺序
  并存，互不覆盖；每笔观测都能定位到触发时的配置或采样。
- 延迟监控只消费这里的延迟来源明细，按观测生成实时延迟告警和健康评估，
  告警条件、级别与健康评分沿用原口径。
- 处置以观测为单位：一轮只处理一笔观测；一笔观测内的处置（实时延迟告警、
  健康评估）分步推进，已完成的步骤幂等跳过，编排中断后可按观测续办未完成步骤。
"""
from __future__ import annotations

from typing import Any

from app.store import store

# 延迟来源类型
SOURCE_CACHE = "cache_invalidation"   # 维表缓存失效（来源定位到关联配置）
SOURCE_SAMPLING = "monitor_sampling"  # 延迟监控采样（来源定位到采样指标）

SOURCE_LABELS = {
    SOURCE_CACHE: "维表缓存失效",
    SOURCE_SAMPLING: "延迟监控采样",
}

# 观测内处置步骤（顺序即处置顺序）
STEP_ALERT = "实时延迟告警"
STEP_HEALTH = "健康评估"

STATUS_PENDING = "待处置"
STATUS_RUNNING = "处置中"
STATUS_DONE = "已完成"


class LagSourceService:
    """延迟来源明细注册表（进程内单例，与内存仓库同生命周期）。"""

    def __init__(self) -> None:
        self._observations: list[dict[str, Any]] = []

    # ---- 登记与查询 ----

    def register_observation(
        self,
        *,
        source_type: str,
        data_stream: str,
        lag_seconds: Any,
        lag_status: str,
        threshold: str,
        sample_time: str,
        lag_row_id: int | None,
        source_ref_id: int,
        source_ref_code: str,
        source_basis: dict[str, Any],
    ) -> dict[str, Any]:
        """登记一笔延迟来源观测。相同数据流的不同来源各写各的，互不覆盖。"""
        new_id = max((int(o.get("id", 0)) for o in self._observations), default=0) + 1
        # 仅延迟偏高/严重滞后需要实时延迟告警与健康评估；正常观测没有待处置项。
        steps: list[dict[str, Any]] = []
        if lag_status in ("延迟偏高", "严重滞后"):
            steps = [
                {"step": STEP_ALERT, "状态": STATUS_PENDING, "输出id": None},
                {"step": STEP_HEALTH, "状态": STATUS_PENDING, "输出id": None},
            ]
        observation = {
            "id": new_id,
            "来源编号": f"LAGS-{new_id:04d}",
            "来源类型": source_type,
            "来源说明": SOURCE_LABELS.get(source_type, source_type),
            "数据流": data_stream,
            "延迟秒数": lag_seconds,
            "延迟状态": lag_status,
            "告警阈值": threshold,
            "采样时间": sample_time,
            "来源引用id": source_ref_id,
            "来源引用编号": source_ref_code,
            "来源依据": dict(source_basis),
            "延迟指标id": lag_row_id,
            "观测状态": STATUS_PENDING if steps else STATUS_DONE,
            "处置步骤": steps,
        }
        self._observations.append(observation)
        return observation

    def list_observations(
        self,
        *,
        data_stream: str | None = None,
        sample_time: str | None = None,
        source_type: str | None = None,
        only_pending: bool = False,
    ) -> list[dict[str, Any]]:
        """按数据流/采样/来源查询延迟来源明细，可只看待处置观测。"""
        result = self._observations
        if data_stream is not None:
            result = [o for o in result if str(o.get("数据流", "")) == str(data_stream)]
        if sample_time is not None:
            result = [o for o in result if str(o.get("采样时间", "")) == str(sample_time)]
        if source_type is not None:
            result = [o for o in result if o.get("来源类型") == source_type]
        if only_pending:
            result = [o for o in result if self.pending_steps(o)]
        return result

    def get_observation(self, observation_id: int) -> dict[str, Any] | None:
        for obs in self._observations:
            if int(obs.get("id", 0)) == observation_id:
                return obs
        return None

    def find_for_lag_row(self, lag_row_id: int) -> dict[str, Any] | None:
        """取某条延迟指标最近登记的来源观测（监控侧按指标续办时使用）。"""
        matched = [
            o for o in self._observations
            if o.get("延迟指标id") == lag_row_id
        ]
        return matched[-1] if matched else None

    @staticmethod
    def pending_steps(observation: dict[str, Any]) -> list[dict[str, Any]]:
        return [s for s in observation.get("处置步骤", []) if s.get("状态") != STATUS_DONE]

    def pending_dispositions(self, observation_id: int) -> list[dict[str, Any]]:
        """按观测查询仍待完成的处置（编排中断后续办用）。"""
        obs = self.get_observation(observation_id)
        if obs is None:
            return []
        return [
            {"step": s["step"], "状态": s["状态"]}
            for s in self.pending_steps(obs)
        ]

    # ---- 处置执行（共用产出：实时延迟告警 + 健康评估，口径沿用原实现） ----

    def _create_realtime_alert(self, obs: dict[str, Any]) -> dict[str, Any]:
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        severity = "P1-紧急" if obs.get("延迟状态") == "严重滞后" else "P2-警告"
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "实时延迟",
            "告警条件": f"延迟 > {obs.get('告警阈值', '30s')}",
            "告警级别": severity,
            "通知渠道": "飞书群+电话",
            "聚合窗口": "1min",
            "静默时段": "无",
            "触发次数": 5,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
            # 来源定位：告警始终指向生成它的那笔延迟来源观测
            "延迟来源id": obs["id"],
            "延迟来源编号": obs["来源编号"],
            "来源类型": obs["来源类型"],
            "来源引用id": obs["来源引用id"],
            "来源引用编号": obs["来源引用编号"],
            "数据流": obs["数据流"],
        }
        alert_rows.append(alert)
        return alert

    def _create_health(self, obs: dict[str, Any]) -> dict[str, Any]:
        health_rows = store.rows("stream_health")
        new_health_id = max((int(r.get("id", 0)) for r in health_rows), default=0) + 1
        score = 45 if obs.get("延迟状态") == "严重滞后" else 72
        health = {
            "id": new_health_id,
            "评估编号": f"STRE-{new_health_id:04d}",
            "数据流": obs["数据流"],
            "健康分数": score,
            "风险项": "高延迟+水位堆积",
            "修复建议": "扩容处理节点+扩大乱序窗口",
            "评估时间": "2026-11-18 10:40:00",
            "下次评估": "2026-11-18 10:45:00",
            "评估状态": "待修复",
            "status": "需修复",
            "pending": True,
            "abnormal": True,
            # 来源定位：评估始终指向生成它的那笔延迟来源观测
            "延迟来源id": obs["id"],
            "延迟来源编号": obs["来源编号"],
            "来源类型": obs["来源类型"],
            "来源引用id": obs["来源引用id"],
            "来源引用编号": obs["来源引用编号"],
        }
        health_rows.append(health)
        return health

    def _run_step(self, obs: dict[str, Any], step: dict[str, Any]) -> dict[str, Any]:
        """执行单笔观测内的一个处置步骤；已完成步骤幂等跳过。"""
        if step.get("状态") == STATUS_DONE:
            return {"step": step["step"], "skipped": True, "output": None}
        if obs.get("观测状态") != STATUS_RUNNING:
            obs["观测状态"] = STATUS_RUNNING
        if step["step"] == STEP_ALERT:
            output = self._create_realtime_alert(obs)
        elif step["step"] == STEP_HEALTH:
            output = self._create_health(obs)
        else:
            raise ValueError(f"未知处置步骤：{step['step']}")
        step["状态"] = STATUS_DONE
        step["输出id"] = output.get("id")
        if not self.pending_steps(obs):
            obs["观测状态"] = STATUS_DONE
        return {"step": step["step"], "skipped": False, "output": output}

    def run_next_step(self, observation_id: int) -> dict[str, Any] | None:
        """只推进一笔观测的下一个处置步骤，用于中断/续办的最小粒度。"""
        obs = self.get_observation(observation_id)
        if obs is None:
            return None
        pending = self.pending_steps(obs)
        if not pending:
            return None
        return self._run_step(obs, pending[0])

    def run_next_observation(self) -> dict[str, Any] | None:
        """一轮处理量：按登记顺序取最早一笔有待处置步骤的观测，跑完其剩余步骤。

        同一轮只处置一笔观测；被延后的观测保留来源和待处置资格，等待后续轮次。
        """
        for obs in self._observations:
            pending = self.pending_steps(obs)
            if not pending:
                continue
            executed: list[dict[str, Any]] = []
            for step in list(pending):
                executed.append(self._run_step(obs, step))
            return {
                "observation": obs,
                "executed": executed,
                "alert": next(
                    (e["output"] for e in executed
                     if e["step"] == STEP_ALERT and e.get("output")),
                    None,
                ),
                "health": next(
                    (e["output"] for e in executed
                     if e["step"] == STEP_HEALTH and e.get("output")),
                    None,
                ),
            }
        return None


# 进程内共享单例：维表关联与延迟监控通过它归集/消费同一份延迟来源明细
lag_source_service = LagSourceService()
