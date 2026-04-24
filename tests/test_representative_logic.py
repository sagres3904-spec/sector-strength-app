import json
import unittest
from pathlib import Path

import pandas as pd

import sector_app_jq as app


def _candidate(
    code,
    name,
    live_ret,
    score,
    *,
    positive_count,
    negative_count,
    hard=False,
    sector_median=0.0,
    return_percentile=0.5,
    return_rank=1,
    market_positive_rate=0.5,
    selected_from="primary",
    live_turnover=100000000.0,
    sector_turnover_share=0.5,
    liquidity_ok=True,
    exclude_spike=False,
):
    centrality = score / 3.0
    today_leadership = score / 2.0
    return {
        "sector_name": "卸売業",
        "today_rank": 1,
        "representative_rank": 0,
        "code": code,
        "name": name,
        "live_price": 100.0,
        "current_price": 100.0,
        "current_price_unavailable": False,
        "live_ret_vs_prev_close": live_ret,
        "live_ret_from_open": live_ret / 2.0,
        "live_turnover": live_turnover,
        "live_turnover_value": live_turnover,
        "live_turnover_unavailable": False,
        "stock_turnover_share_of_sector": sector_turnover_share,
        "rep_score_total": score,
        "rep_score_centrality": centrality,
        "rep_score_today_leadership": today_leadership,
        "rep_score_sanity": 1.0,
        "rep_selected_reason": "",
        "rep_excluded_reason": "",
        "rep_fallback_reason": "",
        "representative_selected_reason": "",
        "representative_quality_flag": "excluded",
        "representative_fallback_reason": "",
        "earnings_today_announcement_flag": False,
        "earnings_announcement_date": "",
        "was_in_selected50": True,
        "was_in_must_have": False,
        "nikkei_search": "",
        "material_link": "",
        "liquidity_ok": liquidity_ok,
        "exclude_spike": exclude_spike,
        "rep_hard_block": hard,
        "hard_block_reason": "",
        "rep_relative_leadership_pass": True,
        "rep_centrality_pass": True,
        "rep_sanity_pass": True,
        "rep_relative_weak": False,
        "sector_candidate_count": positive_count + negative_count,
        "sector_positive_candidate_count": positive_count,
        "sector_negative_candidate_count": negative_count,
        "sector_positive_count": positive_count,
        "sector_negative_count": negative_count,
        "sector_positive_rate": positive_count / max(positive_count + negative_count, 1),
        "sector_live_ret_median": sector_median,
        "sector_median_return": sector_median,
        "sector_top_quartile_return": max(live_ret, sector_median),
        "sector_bottom_quartile_return": min(live_ret, sector_median),
        "sector_top_positive_count": positive_count,
        "stock_return_percentile_in_sector": return_percentile,
        "stock_return_rank_in_sector": return_rank,
        "market_positive_rate": market_positive_rate,
        "market_context": "",
        "sector_context": "",
        "sector_live_ret_pct": return_percentile,
        "sector_today_flow_pct": 0.5,
        "sector_turnover_share": sector_turnover_share,
        "centrality_score": centrality,
        "liquidity_score": 1.0,
        "today_leadership_score": today_leadership,
        "representative_final_score": score,
        "selected_reason": "",
        "selected_horizon": "today",
        "selected_universe": "test",
        "selected_from_primary_or_supplemental": selected_from,
        "primary_candidate_count": 1 if selected_from != "supplemental" else 0,
        "supplemental_candidate_count": 1 if selected_from == "supplemental" else 0,
        "final_candidate_count": positive_count + negative_count,
        "sector_constituent_count": 100,
        "representative_pool_coverage_rate": (positive_count + negative_count) / 100,
        "candidate_pool_warning": "",
        "candidate_pool_reason": "",
        "candidate_in_universe": True,
    }


def _select_today(rows):
    frame = app._apply_today_representative_gate(pd.DataFrame(rows))
    frame["rep_quality_pass"] = (
        ~frame["rep_hard_block"].fillna(False)
        & frame["rep_relative_leadership_pass"].fillna(False)
        & frame["rep_centrality_pass"].fillna(False)
        & frame["rep_sanity_pass"].fillna(False)
    )
    return app._build_sector_representatives(frame)


class RepresentativeLogicTests(unittest.TestCase):
    def test_today_weak_market_allows_small_negative_relative_leader(self):
        rows = [
            _candidate("1001", "A", -0.5, 20.0, positive_count=0, negative_count=4, sector_median=-2.0, return_percentile=0.9, return_rank=1, market_positive_rate=0.2),
            _candidate("1002", "B", -2.5, 10.0, positive_count=0, negative_count=4, sector_median=-2.0, return_percentile=0.4, return_rank=2, market_positive_rate=0.2),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "1001")
        self.assertEqual(selected.iloc[0]["representative_quality_flag"], "quality_pass")

    def test_today_positive_peers_block_event_drop_high_flow_stock(self):
        rows = [
            _candidate("7426", "山大", -11.2, 99.0, positive_count=3, negative_count=1, sector_median=9.8, return_percentile=0.25, return_rank=4),
            _candidate("2737", "トーメンデバイス", 19.0, 20.0, positive_count=3, negative_count=1, sector_median=9.8, return_percentile=0.75, return_rank=2),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "2737")
        self.assertNotIn("7426", set(selected["code"].astype(str)))

    def test_material_backed_breakout_is_not_hard_rejected(self):
        row = _candidate(
            "2737",
            "トーメンデバイス",
            19.06,
            28.0,
            positive_count=4,
            negative_count=1,
            sector_median=3.3,
            return_percentile=0.8,
            return_rank=2,
            live_turnover=16_649_500_000.0,
            sector_turnover_share=0.9837,
            exclude_spike=True,
        )
        gated = app._apply_today_representative_gate(pd.DataFrame([row]))
        self.assertFalse(bool(gated.iloc[0]["rep_hard_block"]))
        self.assertTrue(bool(gated.iloc[0]["representative_gate_pass"]))
        self.assertTrue(bool(gated.iloc[0]["material_supported_breakout"]))
        self.assertTrue(bool(gated.iloc[0]["exclude_spike_warning_only"]))
        self.assertFalse(bool(gated.iloc[0]["exclude_spike_hard_reject"]))
        self.assertIn("exclude_spike_warning_only", gated.iloc[0]["representative_gate_reason"])

    def test_poor_quality_spike_is_rejected(self):
        row = _candidate(
            "9941",
            "太洋物産",
            22.24,
            7.0,
            positive_count=4,
            negative_count=1,
            sector_median=3.3,
            return_percentile=1.0,
            return_rank=1,
            live_turnover=177_900_000.0,
            sector_turnover_share=0.0105,
            exclude_spike=True,
        )
        gated = app._apply_today_representative_gate(pd.DataFrame([row]))
        self.assertTrue(bool(gated.iloc[0]["rep_hard_block"]))
        self.assertFalse(bool(gated.iloc[0]["representative_gate_pass"]))
        self.assertTrue(bool(gated.iloc[0]["poor_quality_spike"]))
        self.assertTrue(bool(gated.iloc[0]["exclude_spike_hard_reject"]))
        self.assertIn("poor_quality_spike", gated.iloc[0]["hard_reject_reason"])

    def test_wholesale_1530_replay_spike_quality_audit(self):
        rows = [
            _candidate("7426", "山大", -11.2, 9.0, positive_count=4, negative_count=1, sector_median=3.297, return_percentile=0.2, return_rank=5, live_turnover=244_000_000.0, sector_turnover_share=0.0144),
            _candidate("9941", "太洋物産", 22.24, 18.0, positive_count=4, negative_count=1, sector_median=3.297, return_percentile=1.0, return_rank=1, live_turnover=177_900_000.0, sector_turnover_share=0.0105, exclude_spike=True),
            _candidate("2737", "トーメンデバイス", 19.06, 30.0, positive_count=4, negative_count=1, sector_median=3.297, return_percentile=0.8, return_rank=2, live_turnover=16_649_500_000.0, sector_turnover_share=0.9837, exclude_spike=True),
            _candidate("8058", "三菱商事", 3.3, 24.0, positive_count=4, negative_count=1, sector_median=3.297, return_percentile=0.6, return_rank=3, live_turnover=53_842_386_600.0, sector_turnover_share=0.20, selected_from="supplemental"),
        ]
        frame = app._apply_today_representative_gate(pd.DataFrame(rows))
        by_code = frame.set_index("code")
        self.assertTrue(bool(by_code.loc["7426", "rep_hard_block"]))
        self.assertFalse(bool(by_code.loc["2737", "rep_hard_block"]))
        self.assertTrue(bool(by_code.loc["2737", "material_supported_breakout"]))
        self.assertTrue(bool(by_code.loc["9941", "poor_quality_spike"]))
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "2737")
        self.assertGreater(float(by_code.loc["2737", "representative_final_score"]), float(by_code.loc["8058", "representative_final_score"]))

    def test_today_strong_sector_prefers_upper_return_group(self):
        rows = [
            _candidate("A", "Upper", 4.0, 20.0, positive_count=3, negative_count=1, sector_median=2.0, return_percentile=0.8, return_rank=1),
            _candidate("B", "Lower", -1.0, 99.0, positive_count=3, negative_count=1, sector_median=2.0, return_percentile=0.2, return_rank=4),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "A")

    def test_today_all_negative_is_flagged_or_least_negative(self):
        rows = [
            _candidate("1001", "A", -1.0, 10.0, positive_count=0, negative_count=2),
            _candidate("1002", "B", -2.0, 9.0, positive_count=0, negative_count=2),
        ]
        selected = _select_today(rows)
        self.assertIn(selected.iloc[0]["code"], {"1001", ""})
        if selected.iloc[0]["code"] == "":
            self.assertEqual(selected.iloc[0]["representative_quality_flag"], "no_valid_today_representative")

    def test_today_fallback_cannot_bypass_hard_negative_gate(self):
        rows = [_candidate("7426", "山大", -11.2, 99.0, positive_count=3, negative_count=1, sector_median=9.8, return_percentile=0.25, return_rank=4)]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "")
        self.assertEqual(selected.iloc[0]["representative_quality_flag"], "no_valid_today_representative")

    def test_today_light_negative_allowed_when_sector_positive_rate_is_low(self):
        rows = [
            _candidate("A", "Relative", -0.8, 20.0, positive_count=1, negative_count=4, sector_median=-2.0, return_percentile=0.85, return_rank=1, market_positive_rate=0.3),
            _candidate("B", "Weak", -3.0, 10.0, positive_count=1, negative_count=4, sector_median=-2.0, return_percentile=0.3, return_rank=4, market_positive_rate=0.3),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "A")

    def test_representative_supplemental_pool_adds_center_candidates(self):
        pool = pd.DataFrame(
            [
                {"code": "A", "sector_name": "S", "must_have_rank_in_sector": 1, "protected_sector_rank": 1, "must_have_representative_support": 10.0, "sector_contribution_full": 0.5, "TradingValue_latest": 1000.0, "avg_turnover_20d": 1000.0},
                {"code": "B", "sector_name": "S", "must_have_rank_in_sector": 2, "protected_sector_rank": 1, "must_have_representative_support": 9.0, "sector_contribution_full": 0.4, "TradingValue_latest": 900.0, "avg_turnover_20d": 900.0},
                {"code": "C", "sector_name": "S", "must_have_rank_in_sector": 3, "protected_sector_rank": 1, "must_have_representative_support": 8.0, "sector_contribution_full": 0.3, "TradingValue_latest": 800.0, "avg_turnover_20d": 800.0},
            ]
        )
        supplemental = app._build_deep_watch_representative_supplemental_pool(pool)
        self.assertEqual(supplemental["code"].tolist(), ["C"])

    def test_supplemental_large_drop_is_still_rejected_by_gate(self):
        rows = [
            _candidate("S1", "SupplementalDrop", -11.2, 99.0, positive_count=2, negative_count=1, sector_median=4.0, return_percentile=0.2, return_rank=3, selected_from="supplemental"),
            _candidate("P1", "PrimaryGood", 3.0, 10.0, positive_count=2, negative_count=1, sector_median=4.0, return_percentile=0.7, return_rank=2),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "P1")
        self.assertNotIn("S1", set(selected["code"].astype(str)))

    def test_bad_primary_candidates_can_be_replaced_by_valid_supplemental(self):
        rows = [
            _candidate("P1", "PrimaryBad", -9.0, 99.0, positive_count=1, negative_count=1, sector_median=1.0, return_percentile=0.2, return_rank=2),
            _candidate("S1", "SupplementalGood", 2.0, 20.0, positive_count=1, negative_count=1, sector_median=1.0, return_percentile=1.0, return_rank=1, selected_from="supplemental"),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "S1")

    def test_no_valid_primary_or_supplemental_returns_none(self):
        rows = [
            _candidate("P1", "PrimaryBad", -9.0, 99.0, positive_count=0, negative_count=2, sector_median=-1.0, return_percentile=0.2, return_rank=2),
            _candidate("S1", "SupplementalBad", -8.5, 20.0, positive_count=0, negative_count=2, sector_median=-1.0, return_percentile=0.1, return_rank=2, selected_from="supplemental"),
        ]
        selected = _select_today(rows)
        self.assertEqual(selected.iloc[0]["code"], "")

    def test_1w_representative_rejects_large_negative_stock(self):
        frame = pd.DataFrame(
            [
                {"code": "A", "live_ret_vs_prev_close": -5.1, "rs_vs_topix_1w": 5.0, "ret_1w": 10.0},
                {"code": "B", "live_ret_vs_prev_close": 1.0, "rs_vs_topix_1w": 2.0, "ret_1w": 3.0},
            ]
        )
        filtered = app._apply_1w_representative_gate(frame)
        self.assertEqual(filtered["code"].tolist(), ["B"])

    def test_1m_and_3m_representatives_reject_event_like_large_drop(self):
        frame = pd.DataFrame(
            [
                {"code": "A", "live_ret_vs_prev_close": -7.1, "ret_1m": 20.0, "ret_3m": 30.0, "rs_vs_topix_1m": 5.0, "rs_vs_topix_3m": 7.0, "avg_turnover_20d": 100.0},
                {"code": "B", "live_ret_vs_prev_close": -1.0, "ret_1m": 5.0, "ret_3m": 8.0, "rs_vs_topix_1m": 1.0, "rs_vs_topix_3m": 2.0, "avg_turnover_20d": 100.0},
            ]
        )
        self.assertEqual(app._apply_1m_representative_gate(frame)["code"].tolist(), ["B"])
        self.assertEqual(app._apply_3m_representative_gate(frame)["code"].tolist(), ["B"])

    def test_1m_and_3m_do_not_rehabilitate_event_drop_spike(self):
        frame = pd.DataFrame(
            [
                {"code": "SPIKE", "live_ret_vs_prev_close": -7.5, "ret_1m": 30.0, "ret_3m": 45.0, "rs_vs_topix_1m": 8.0, "rs_vs_topix_3m": 10.0, "avg_turnover_20d": 1000.0},
                {"code": "CORE", "live_ret_vs_prev_close": -0.5, "ret_1m": 6.0, "ret_3m": 9.0, "rs_vs_topix_1m": 1.0, "rs_vs_topix_3m": 2.0, "avg_turnover_20d": 1000.0},
            ]
        )
        self.assertEqual(app._apply_1m_representative_gate(frame)["code"].tolist(), ["CORE"])
        self.assertEqual(app._apply_3m_representative_gate(frame)["code"].tolist(), ["CORE"])

    def test_saved_latest_snapshot_display_tables_do_not_break(self):
        for name in ["latest_0915.json", "latest_1130.json", "latest_1530.json"]:
            data = json.loads((Path("data/snapshots") / name).read_text(encoding="utf-8"))
            leaderboard = pd.DataFrame(data.get("today_sector_leaderboard", []))
            reps = pd.DataFrame(data.get("sector_representatives", []))
            display = app._build_sector_representatives_display_frame(reps, today_sector_leaderboard=leaderboard)
            self.assertIn("sector_name", display.columns)
            for horizon in ["1w", "1m", "3m"]:
                swing_display = app._build_swing_candidate_display_frame(pd.DataFrame(data.get(f"swing_candidates_{horizon}", [])), horizon=horizon)
                self.assertIn("sector_name", swing_display.columns)

    def test_saved_horizon_representative_codes_are_resolved(self):
        data = json.loads(Path("data/snapshots/latest_1530.json").read_text(encoding="utf-8"))
        for horizon in ["1w", "1m", "3m"]:
            missing = 0
            for row in data.get(f"sector_persistence_{horizon}", []):
                for item in row.get("representative_stocks", []) or []:
                    if isinstance(item, dict) and not str(item.get("code", "")).strip():
                        missing += 1
            self.assertEqual(missing, 0, horizon)


if __name__ == "__main__":
    unittest.main()
