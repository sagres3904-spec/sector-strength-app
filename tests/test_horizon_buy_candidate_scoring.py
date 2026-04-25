import json
import unittest
from pathlib import Path

import pandas as pd

import sector_app_jq as app


def _row(
    code,
    name,
    sector,
    *,
    live_ret=0.5,
    ret_1w=1.0,
    ret_1m=2.0,
    ret_3m=3.0,
    rs_1w=1.0,
    rs_1m=1.0,
    rs_3m=1.0,
    price_vs_ma20=3.0,
    turnover=1_000_000_000.0,
    volume=1_000_000.0,
    live_turnover_ratio=1.2,
    live_volume_ratio=1.2,
    high_close=0.9,
    earnings_days=20,
    earnings_today=False,
    finance=0.0,
    sector_contribution=0.15,
    turnover_rank=2,
    must_have_rank=2,
    was_must_have=False,
):
    return {
        "code": code,
        "name": name,
        "sector_name": sector,
        "current_price": 100.0,
        "live_price": 100.0,
        "live_ret_vs_prev_close": live_ret,
        "live_ret_from_open": live_ret / 2.0,
        "gap_pct": live_ret / 3.0,
        "live_turnover_value": turnover,
        "live_turnover": turnover,
        "avg_turnover_20d": turnover,
        "avg_volume_20d": volume,
        "TradingValue_latest": turnover,
        "live_turnover_ratio_20d": live_turnover_ratio,
        "live_volume_ratio_20d": live_volume_ratio,
        "high_close_score": high_close,
        "price_vs_ma20_pct": price_vs_ma20,
        "rs_vs_topix_1w": rs_1w,
        "rs_vs_topix_1m": rs_1m,
        "rs_vs_topix_3m": rs_3m,
        "ret_1w": ret_1w,
        "ret_1m": ret_1m,
        "ret_3m": ret_3m,
        "sector_rank_1w": 1,
        "sector_rank_1m": 1,
        "sector_rank_3m": 1,
        "earnings_buffer_days": earnings_days,
        "earnings_today_announcement_flag": earnings_today,
        "earnings_announcement_date": "2026-05-20",
        "finance_health_score": finance,
        "sector_contribution_full": sector_contribution,
        "turnover_rank_in_sector": turnover_rank,
        "must_have_rank_in_sector": must_have_rank,
        "was_in_must_have": was_must_have,
        "nikkei_search": "",
        "material_link": "",
    }


def _persistence_frame(ranks):
    rows = []
    for sector, rank in ranks:
        rows.append(
            {
                "sector_name": sector,
                "persistence_rank": rank,
                "sector_confidence": "高" if rank <= 5 else "中",
                "sector_gate_pass": True,
                "sector_positive_ratio": 0.70 if rank <= 5 else 0.55,
                "leader_concentration_share": 0.30,
            }
        )
    return pd.DataFrame(rows)


def _build_tables():
    rows = [
        _row("1001", "短期モメンタム", "短期強", live_ret=2.4, ret_1w=4.5, rs_1w=3.5, rs_1m=1.0, price_vs_ma20=4.0, live_turnover_ratio=1.8),
        _row("1002", "決算過熱", "短期強", live_ret=9.0, ret_1w=14.0, rs_1w=8.0, price_vs_ma20=17.0, live_turnover_ratio=2.3, earnings_days=0, earnings_today=True),
        _row("2001", "中期トレンド", "中期強", live_ret=0.6, ret_1m=7.0, ret_3m=12.0, rs_1m=7.0, rs_3m=9.0, price_vs_ma20=3.0, live_turnover_ratio=1.1),
        _row("2002", "一日急騰", "中期強", live_ret=9.0, ret_1w=18.0, ret_1m=8.0, ret_3m=10.0, rs_1m=8.0, rs_3m=8.0, price_vs_ma20=18.0, turnover=120_000_000.0, live_turnover_ratio=2.6, sector_contribution=0.02, turnover_rank=12, must_have_rank=12),
        _row("8058", "三菱商事", "長期強", live_ret=0.4, ret_1m=3.0, ret_3m=9.0, rs_1m=2.0, rs_3m=6.0, price_vs_ma20=2.0, turnover=5_000_000_000.0, volume=4_000_000.0, live_turnover_ratio=1.0, sector_contribution=0.40, turnover_rank=1, must_have_rank=1, was_must_have=True),
        _row("3002", "小型急騰", "長期強", live_ret=10.0, ret_1w=20.0, ret_1m=12.0, ret_3m=30.0, rs_1m=8.0, rs_3m=14.0, price_vs_ma20=19.0, turnover=80_000_000.0, live_turnover_ratio=2.8, sector_contribution=0.01, turnover_rank=18, must_have_rank=18),
    ]
    leaderboard = pd.DataFrame(
        [
            {"sector_name": "短期強", "today_rank": 1, "sector_confidence": "高"},
            {"sector_name": "中期強", "today_rank": 3, "sector_confidence": "高"},
            {"sector_name": "長期強", "today_rank": 6, "sector_confidence": "中"},
        ]
    )
    persistence = {
        "1w": _persistence_frame([("短期強", 1), ("中期強", 4), ("長期強", 8)]),
        "1m": _persistence_frame([("中期強", 1), ("長期強", 4), ("短期強", 11)]),
        "3m": _persistence_frame([("長期強", 1), ("中期強", 4), ("短期強", 14)]),
    }
    return app._build_swing_candidate_tables_v2(pd.DataFrame(rows), leaderboard, persistence)


class HorizonBuyCandidateScoringTests(unittest.TestCase):
    def test_1w_rewards_short_momentum_but_penalizes_overheated_earnings_day(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_1w"]).set_index("code")
        self.assertGreater(audit.loc["1001", "buy_score_1w"], audit.loc["1002", "buy_score_1w"])
        self.assertLess(audit.loc["1002", "earnings_risk_score"], 0)
        self.assertLess(audit.loc["1002", "overheating_penalty"], 0)
        self.assertIn("1001", set(tables["1w"]["code"].astype(str)))

    def test_1m_prefers_medium_trend_over_one_day_spike(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_1m"]).set_index("code")
        self.assertGreater(audit.loc["2001", "buy_score_1m"], audit.loc["2002", "buy_score_1m"])
        self.assertLess(audit.loc["2002", "overheating_penalty"], 0)
        selected = set(tables["1m"].loc[~tables["1m"]["fallback_used"].fillna(False), "code"].astype(str))
        self.assertIn("2001", selected)

    def test_3m_prefers_core_stock_and_does_not_promote_small_short_spike(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_3m"]).set_index("code")
        self.assertGreater(audit.loc["8058", "buy_score_3m"], audit.loc["3002", "buy_score_3m"])
        self.assertLess(audit.loc["3002", "overheating_penalty"], 0)
        selected = set(tables["3m"].loc[~tables["3m"]["fallback_used"].fillna(False), "code"].astype(str))
        self.assertIn("8058", selected)

    def test_candidate_audit_fields_and_snapshot_policy_remain_present(self):
        tables = _build_tables()
        for horizon in ["1w", "1m", "3m"]:
            frame = tables[horizon]
            self.assertEqual(frame["code"].astype(str).str.strip().eq("").sum(), 0)
            self.assertIn(f"buy_score_{horizon}", frame.columns)
            self.assertIn("score_components", frame.columns)
        self.assertFalse(tables["3m"]["fallback_used"].fillna(False).all())
        data = json.loads(Path("data/snapshots/latest_1530.json").read_text(encoding="utf-8"))
        meta = data.get("meta", {})
        self.assertEqual(meta.get("edinetdb_calendar_status"), "ok")
        self.assertFalse(bool(meta.get("edinetdb_calendar_jquants_fallback_used")))
        self.assertFalse(bool(meta.get("earnings_announcement_jquants_fallback_used")))


if __name__ == "__main__":
    unittest.main()
