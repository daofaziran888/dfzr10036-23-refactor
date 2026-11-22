"""延迟来源归集场景验证：覆盖缓存失效/监控采样两类观测的隔离、中断续办与一轮一笔。

运行（在 backend 目录）：
    .venv/bin/python -I tests/test_lag_source_scenario.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.orchestrator import (  # noqa: E402
    dim_join,
    flow_cache_invalidation,
    flow_lag_recovery,
    flow_lag_source_pending,
    flow_lag_source_resume,
    flow_lag_source_step,
    flow_record_sampling,
    lag_monitor,
)
from app.services.lag_source import (  # noqa: E402
    SOURCE_CACHE,
    SOURCE_SAMPLING,
    lag_source_service,
)
from app.store import store  # noqa: E402

ORDER_STREAM = "order_events_cdc"
PASS = "[PASS]"
FAIL = "[FAIL]"
failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = PASS if condition else FAIL
    print(f"{mark} {name}" + (f" :: {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def find(rows: list[dict], **criteria) -> dict | None:
    for row in rows:
        if all(row.get(k) == v for k, v in criteria.items()):
            return row
    return None


def main() -> int:
    # ---- 前置：订单流（order_events_cdc）延迟指标初始为正常，延迟 0.8s ----
    order_lm = find(store.rows("lag_monitor"), 数据流=ORDER_STREAM)
    check("前置：订单流延迟指标存在且正常", order_lm is not None
          and order_lm["status"] == "正常", f"id={order_lm and order_lm['id']}")
    initial_lag = order_lm["延迟秒数"]

    # 订单流关联配置置为缓存失效（模拟重建缓存）
    order_dim = find(store.rows("dim_join"), 事实表=ORDER_STREAM)
    order_dim["status"] = "缓存失效"
    order_dim["abnormal"] = True

    # ===== 场景1：缓存失效 -> 写入45s延迟偏高 + 缓存命中率告警 =====
    r1 = flow_cache_invalidation(order_dim["id"])
    check("场景1：缓存失效编排成功", r1["ok"] is True, str(r1.get("trace")))
    check("场景1：订单流延迟被改为45s延迟偏高",
          order_lm["status"] == "延迟偏高" and order_lm["延迟秒数"] == 45)
    cache_alert = find(store.rows("alert_rule"), 监控指标="维表缓存命中率",
                       来源引用编号=order_dim["配置编号"])
    check("场景1：缓存命中率告警可回查配置依据",
          cache_alert is not None
          and cache_alert.get("来源类型") == SOURCE_CACHE
          and cache_alert.get("来源引用id") == order_dim["id"],
          str(cache_alert and {k: cache_alert[k] for k in
                               ("监控指标", "来源类型", "来源引用编号")}))
    cache_obs = lag_source_service.list_observations(
        data_stream=ORDER_STREAM, source_type=SOURCE_CACHE)[-1]
    check("场景1：缓存观测已入共用归集，含触发配置依据",
          cache_obs["来源依据"]["配置编号"] == order_dim["配置编号"]
          and cache_obs["延迟状态"] == "延迟偏高"
          and cache_obs["延迟秒数"] == 45)

    # ===== 场景2：监控采集随即报出独立的严重滞后（同数据流，第二笔观测）=====
    order_lm["延迟秒数"] = 1230
    r2 = flow_record_sampling(order_lm["id"], "严重滞后")
    check("场景2：监控采样观测登记成功", r2["ok"] is True)
    sample_obs = r2["observation"]
    check("场景2：采样观测与缓存观测相互独立、不覆盖",
          sample_obs["id"] != cache_obs["id"]
          and sample_obs["来源类型"] == SOURCE_SAMPLING
          and cache_obs["延迟秒数"] == 45
          and sample_obs["延迟秒数"] == 1230
          and sample_obs["延迟状态"] == "严重滞后",
          f"cache={cache_obs['id']}/45s, sample={sample_obs['id']}/1230s")
    check("场景2：采样来源可定位到触发时采样",
          sample_obs["来源依据"]["指标编号"] == order_lm["指标编号"]
          and sample_obs["来源引用id"] == order_lm["id"])

    pending_two = lag_source_service.list_observations(
        data_stream=ORDER_STREAM, only_pending=True)
    check("场景2：同数据流两笔观测都保留待处置资格",
          len(pending_two) == 2
          and [o["id"] for o in pending_two] == [cache_obs["id"], sample_obs["id"]])

    # ===== 场景3：实时延迟告警写入后中断（只走告警一步）=====
    step1 = flow_lag_source_step(sample_obs["id"])
    check("场景3：可指定采样观测先推进实时延迟告警一步",
          step1["ok"] and step1["step"] == "实时延迟告警")
    alert_of_sample = step1["output"]
    check("场景3：告警指向采样观测（严重滞后 P1）",
          alert_of_sample["延迟来源id"] == sample_obs["id"]
          and alert_of_sample["告警级别"] == "P1-紧急"
          and alert_of_sample["监控指标"] == "实时延迟")
    check("场景3：中断后该观测只剩健康评估待处置",
          [d["step"] for d in lag_source_service.pending_dispositions(sample_obs["id"])]
          == ["健康评估"])

    # ===== 场景4：按原采样查询待处置，只有健康评估；续办后清空 =====
    q = flow_lag_source_pending(
        data_stream=ORDER_STREAM, sample_time=order_lm["最近采样"])
    sample_items = [it for it in q["items"]
                    if it["observation"]["id"] == sample_obs["id"]]
    check("场景4：按原采样查询到采样观测，待处置只有健康评估",
          len(sample_items) == 1
          and [d["step"] for d in sample_items[0]["pending_dispositions"]]
          == ["健康评估"])
    resume = flow_lag_source_resume(sample_obs["id"])
    check("场景4：续办成功且待处理项清空",
          resume["ok"] and resume["pending_dispositions"] == []
          and resume["observation"]["观测状态"] == "已完成",
          str(resume.get("trace")))
    health_of_sample = find(store.rows("stream_health"),
                            延迟来源id=sample_obs["id"])
    check("场景4：独立严重滞后产生对应健康评估，评分45（沿用原口径）",
          health_of_sample is not None and health_of_sample["健康分数"] == 45)
    check("场景4：原告警来源不变",
          find(store.rows("alert_rule"), id=alert_of_sample["id"])["延迟来源id"]
          == sample_obs["id"])
    # 再续办一次：幂等，不重复产出
    again = flow_lag_source_resume(sample_obs["id"])
    check("场景4：重复续办幂等、无新增写出", again["ok"] and again["trace"] == [])

    # ===== 场景5：对已严重滞后的订单流重建缓存，原延迟值保持 =====
    r5 = flow_cache_invalidation(order_dim["id"])
    check("场景5：严重滞后时重建缓存仍登记缓存观测", r5["ok"] is True)
    check("场景5：缓存失效只更新原来允许更新的记录，严重滞后延迟值保持1230",
          order_lm["status"] == "严重滞后" and order_lm["延迟秒数"] == 1230,
          f"status={order_lm['status']}, 延迟={order_lm['延迟秒数']}")
    new_cache_obs = lag_source_service.list_observations(
        data_stream=ORDER_STREAM, source_type=SOURCE_CACHE)[-1]
    check("场景5：被延后的缓存观测保留来源与待处置资格",
          new_cache_obs["id"] not in (cache_obs["id"], sample_obs["id"])
          and lag_source_service.pending_dispositions(new_cache_obs["id"])
          and new_cache_obs["来源引用编号"] == order_dim["配置编号"])

    # 当前待处置队列（FIFO）：第一笔缓存观测 -> 第二笔缓存观测（采样观测已完成）
    queue = lag_source_service.list_observations(
        data_stream=ORDER_STREAM, only_pending=True)
    check("积压：剩余两笔缓存观测按登记顺序排队",
          [o["id"] for o in queue] == [cache_obs["id"], new_cache_obs["id"]],
          str([o["id"] for o in queue]))

    # ===== 场景6：从两笔都待处置的积压恢复，按一次处理量跑一轮 =====
    alerts_before = len(store.rows("alert_rule"))
    health_before = len(store.rows("stream_health"))
    round1 = flow_lag_recovery(1)
    check("场景6：一轮恢复成功", round1["ok"] is True, str(round1.get("trace")))
    handled_obs = lag_source_service.get_observation(cache_obs["id"])
    check("场景6：本轮只处置最早一笔（第一笔缓存观测）",
          handled_obs["观测状态"] == "已完成"
          and lag_source_service.get_observation(new_cache_obs["id"])["观测状态"]
          == "待处置")
    round_alert = find(store.rows("alert_rule"), 延迟来源id=cache_obs["id"])
    round_health = find(store.rows("stream_health"), 延迟来源id=cache_obs["id"])
    check("场景6：本轮实时延迟告警来自第一笔缓存采样（45s延迟偏高 P2）",
          round_alert is not None
          and round_alert["来源类型"] == SOURCE_CACHE
          and round_alert["来源引用编号"] == order_dim["配置编号"]
          and round_alert["告警级别"] == "P2-警告"
          and round_alert["告警条件"] == "延迟 > 30s",
          f"alert={round_alert and round_alert['策略编号']}")
    check("场景6：本轮健康评分为72（延迟偏高沿用原口径）",
          round_health is not None and round_health["健康分数"] == 72)
    check("场景6：本轮恰好写出1条实时延迟告警+1份健康评估",
          len(store.rows("alert_rule")) - alerts_before == 1
          and len(store.rows("stream_health")) - health_before == 1)
    remaining = lag_source_service.list_observations(
        data_stream=ORDER_STREAM, only_pending=True)
    check("场景6：下一轮只剩第二笔缓存观测",
          [o["id"] for o in remaining] == [new_cache_obs["id"]])

    # ===== 场景7：再跑一轮，队列清空，后续空轮不产出 =====
    round2 = flow_lag_recovery(1)
    check("场景7：第二轮处置完最后一笔观测",
          lag_source_service.get_observation(new_cache_obs["id"])["观测状态"]
          == "已完成")
    last_alert = find(store.rows("alert_rule"), 延迟来源id=new_cache_obs["id"])
    check("场景7：最后一笔仍是缓存来源、P2、评分72",
          last_alert is not None and last_alert["来源类型"] == SOURCE_CACHE
          and find(store.rows("stream_health"),
                   延迟来源id=new_cache_obs["id"])["健康分数"] == 72)
    empty = flow_lag_recovery(1)
    check("场景7：队列清空后空轮无告警/评估写出",
          empty["ok"] and not any("alert_rule" in t or "stream_health" in t
                                  for t in empty["trace"]))

    # ===== 旧入口兼容性：escalate 已完成观测不重复产出 =====
    legacy = lag_monitor.escalate_to_alert_and_health(order_lm["id"])
    check("兼容：旧入口对已处置观测不重复产出告警", legacy["alert"] is None)

    print("\n=== 汇总 ===")
    if failures:
        print(f"{len(failures)} 项失败：{failures}")
        return 1
    print("全部场景通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
