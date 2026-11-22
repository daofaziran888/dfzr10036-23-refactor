"""维表关联服务：关联补全后推动水位推进和Schema演进检测。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.lag_source import SOURCE_CACHE, lag_source_service
from app.store import store


class DimJoinService(BaseModuleService):
    MODULE = "dim_join"
    ENTITY = "关联配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["配置编号", "事实表", "维度表", "关联键", "关联类型", "缺失策略", "缓存TTL", "命中率"]
    REQUIRED_FIELDS = ["配置编号", "事实表", "维度表"]
    STATUS_ORDER = ["草稿", "关联中", "缓存失效", "已停止"]
    ACTION_RULES = {"启动关联": "关联中", "重建缓存": "缓存失效", "停止关联": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_watermark_and_schema(self, entry_id: int) -> dict[str, Any]:
        """维表关联启动后，触发水位推进和Schema演进检测。

        数据流链路: dim_join -> watermark_mgr + schema_evo
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "关联中":
            return None

        tag = str(entry.get("事实表", ""))
        results: dict[str, Any] = {"watermark": None, "schema": None}

        # 水位推进
        wm_rows = store.rows("watermark_mgr")
        has_wm = [r for r in wm_rows if str(r.get("数据流", "")) == tag]
        if has_wm:
            has_wm[0]["status"] = "已追上"
            has_wm[0]["pending"] = True
            has_wm[0]["abnormal"] = False
            has_wm[0]["当前位点"] = "binlog.001234:891200"
            has_wm[0]["提交位点"] = "binlog.001234:891200"
            has_wm[0]["延迟行数"] = 0
            results["watermark"] = has_wm[0]
        else:
            new_wm_id = max((int(r.get("id", 0)) for r in wm_rows), default=0) + 1
            wm_config = {
                "id": new_wm_id,
                "水位编号": f"WATE-{new_wm_id:04d}",
                "数据流": tag,
                "当前位点": "binlog.001234:891200",
                "提交位点": "binlog.001234:891200",
                "延迟行数": 0,
                "水位类型": "事件时间水位",
                "最后推进": "2026-11-18 10:32:15",
                "追赶进度": "100%",
                "status": "已追上",
                "pending": True,
                "abnormal": False,
            }
            wm_rows.append(wm_config)
            results["watermark"] = wm_config

        # Schema演进检测
        se_rows = store.rows("schema_evo")
        new_se_id = max((int(r.get("id", 0)) for r in se_rows), default=0) + 1
        schema_rec = {
            "id": new_se_id,
            "记录编号": f"SCHE-{new_se_id:04d}",
            "数据流": tag,
            "变更列": "customer_tier(enum→varchar)",
            "列变更前": "ENUM('bronze','silver','gold')",
            "列变更后": "VARCHAR(32)",
            "兼容模式": "向前兼容",
            "下游影响": "仅影响实时打标任务",
            "演进时间": "2026-11-18 10:32:20",
            "status": "兼容通过",
            "pending": True,
            "abnormal": False,
        }
        se_rows.append(schema_rec)
        results["schema"] = schema_rec

        return results

    def handle_cache_invalidation(self, entry_id: int) -> dict[str, Any]:
        """缓存失效时，发出告警并级联影响下游延迟指标。

        数据流链路: dim_join(缓存失效) -> alert_rule + lag_source(共用延迟来源明细)
        维表关联只把失效依据交给共用归集；延迟记录仍只更新原来允许更新的那一笔，
        同数据流后续的独立采样观测不会被这笔缓存观测覆盖，反之亦然。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "缓存失效":
            return None

        tag = str(entry.get("事实表", ""))
        results = {"alert": None, "lag": None}

        # 创建缓存命中率告警，并回查触发时的配置依据
        alert_rows = store.rows("alert_rule")
        new_alert_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        alert = {
            "id": new_alert_id,
            "策略编号": f"ALER-{new_alert_id:04d}",
            "监控指标": "维表缓存命中率",
            "告警条件": "命中率 < 60% 持续5min",
            "告警级别": "P2-警告",
            "通知渠道": "飞书群+电话",
            "聚合窗口": "5min",
            "静默时段": "0-6点",
            "触发次数": 3,
            "status": "已触发",
            "pending": True,
            "abnormal": True,
            # 来源定位：缓存命中率告警回查当时的关联配置
            "来源类型": SOURCE_CACHE,
            "来源引用id": entry_id,
            "来源引用编号": entry.get("配置编号"),
            "数据流": tag,
        }
        alert_rows.append(alert)
        results["alert"] = alert

        # 标记延迟：只更新原来允许更新的延迟记录（正常/采集中），
        # 已处于延迟偏高/严重滞后的记录不在原口径允许范围内，保持原值不被覆盖。
        target_lm = None
        for lm in store.rows("lag_monitor"):
            if str(lm.get("数据流", "")) == tag and lm.get("status") in ("正常", "采集中"):
                target_lm = lm
                break

        if target_lm is not None:
            target_lm["status"] = "延迟偏高"
            target_lm["pending"] = True
            target_lm["abnormal"] = True
            target_lm["延迟秒数"] = 45
            results["lag"] = target_lm

        # 缓存失效依据交给共用归集：无论是否更新到延迟记录，都登记独立的延迟来源观测。
        # 观测口径固定为缓存引起的 45s 延迟偏高；未命中可更新记录时不触碰原延迟值，
        # 仅保留来源和待处置资格，等后续轮次实时评估。
        lag_observation = lag_source_service.register_observation(
            source_type=SOURCE_CACHE,
            data_stream=tag,
            lag_seconds=45,
            lag_status="延迟偏高",
            threshold=str(target_lm.get("告警阈值", "30s")) if target_lm is not None else "30s",
            sample_time=str(target_lm.get("最近采样", "")) if target_lm is not None else "2026-11-18 10:33:00",
            lag_row_id=target_lm.get("id") if target_lm is not None else None,
            source_ref_id=entry_id,
            source_ref_code=str(entry.get("配置编号", "")),
            source_basis={
                "模块": self.MODULE,
                "配置编号": entry.get("配置编号"),
                "事实表": entry.get("事实表"),
                "维度表": entry.get("维度表"),
                "关联键": entry.get("关联键"),
                "缓存TTL": entry.get("缓存TTL"),
                "命中率": entry.get("命中率"),
                "缺失策略": entry.get("缺失策略"),
                "告警策略编号": alert.get("策略编号"),
            },
        )
        results["lag_source"] = lag_observation

        return results