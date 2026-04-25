import json
import math
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
    earnings_date="2026-05-20",
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
        "earnings_announcement_date": earnings_date,
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
        _row("1003", "決算近接強い", "短期強", live_ret=2.0, ret_1w=5.0, ret_1m=2.0, rs_1w=4.0, rs_1m=2.0, price_vs_ma20=4.0, live_turnover_ratio=1.7, earnings_days=2, earnings_date="2026-04-27"),
        _row("1004", "決算近接過熱", "短期強", live_ret=5.2, ret_1w=9.0, ret_1m=5.0, rs_1w=5.0, rs_1m=3.0, price_vs_ma20=13.0, live_turnover_ratio=2.0, earnings_days=2, earnings_date="2026-04-27"),
        _row("2001", "中期トレンド", "中期強", live_ret=0.6, ret_1m=7.0, ret_3m=12.0, rs_1m=7.0, rs_3m=9.0, price_vs_ma20=3.0, live_turnover_ratio=1.1),
        _row("2002", "一日急騰", "中期強", live_ret=9.0, ret_1w=18.0, ret_1m=8.0, ret_3m=10.0, rs_1m=8.0, rs_3m=8.0, price_vs_ma20=18.0, turnover=120_000_000.0, live_turnover_ratio=2.6, sector_contribution=0.02, turnover_rank=12, must_have_rank=12),
        _row("2003", "決算通過後安定", "中期強", live_ret=0.8, ret_1w=1.5, ret_1m=6.5, ret_3m=11.0, rs_1m=6.0, rs_3m=8.0, price_vs_ma20=2.5, live_turnover_ratio=1.2, earnings_days=-2, earnings_date="2026-04-23"),
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

    def test_candidate_display_retains_buy_reason_and_entry_caution(self):
        tables = _build_tables()
        for horizon in ["1w", "1m", "3m"]:
            frame = tables[horizon].copy()
            self.assertFalse(frame.empty)
            first_index = frame.index[0]
            first_code = str(frame.loc[first_index, "code"])
            frame.loc[first_index, "horizon_fit_reason"] = f"{horizon}検証理由"
            frame.loc[first_index, "entry_caution"] = "決算当日注意"

            display = app._build_swing_candidate_display_frame(frame, horizon=horizon)

            self.assertIn("horizon_fit_reason", display.columns)
            self.assertIn("entry_caution", display.columns)
            displayed_row = display.loc[display["code"].astype(str).eq(first_code)].iloc[0]
            self.assertEqual(displayed_row["horizon_fit_reason"], f"{horizon}検証理由")
            self.assertEqual(displayed_row["entry_caution"], "決算当日注意")

    def test_earnings_near_strong_candidate_is_event_caution_not_hard_blocked(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_1w"]).set_index("code")
        row = audit.loc["1003"]

        self.assertEqual(row["hard_block_reason_raw"], "")
        self.assertTrue(bool(row["event_candidate_flag"]))
        self.assertEqual(row["candidate_bucket"], "event_caution_candidate")
        self.assertEqual(row["candidate_bucket_label"], "イベント注意候補")
        self.assertIn("決算近い", row["event_caution_reason"])
        self.assertGreater(row["buy_strength_score"], 1.0)
        self.assertLess(row["entry_timing_adjustment"], 0.0)
        self.assertGreater(row["entry_timing_adjustment"], -0.5)
        self.assertAlmostEqual(row["buy_score_total"], row["buy_strength_score"] + row["entry_timing_adjustment"], places=6)

    def test_display_retains_event_bucket_fields(self):
        tables = _build_tables()
        for horizon in ["1w", "1m", "3m"]:
            frame = tables[horizon].copy()
            self.assertFalse(frame.empty)
            first_index = frame.index[0]
            first_code = str(frame.loc[first_index, "code"])
            frame.loc[first_index, "candidate_bucket_label"] = "イベント注意候補"
            frame.loc[first_index, "event_caution_reason"] = "決算近い。強さは高いが値動き急変に注意"

            display = app._build_swing_candidate_display_frame(frame, horizon=horizon)

            self.assertIn("candidate_bucket_label", display.columns)
            self.assertIn("event_caution_reason", display.columns)
            displayed_row = display.loc[display["code"].astype(str).eq(first_code)].iloc[0]
            self.assertEqual(displayed_row["candidate_bucket_label"], "イベント注意候補")
            self.assertEqual(displayed_row["event_caution_reason"], "決算近い。強さは高いが値動き急変に注意")

    def test_earnings_near_and_extended_candidate_is_chase_caution(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_1w"]).set_index("code")
        row = audit.loc["1004"]

        self.assertTrue(bool(row["event_candidate_flag"]))
        self.assertEqual(row["candidate_bucket"], "chase_caution_candidate")
        self.assertEqual(row["candidate_bucket_label"], "追いかけ注意")
        self.assertIn("追いかけ注意", row["event_caution_reason"])

    def test_post_earnings_stable_trend_can_be_follow_candidate(self):
        tables = _build_tables()
        audit = pd.DataFrame(tables["audit_1m"]).set_index("code")
        row = audit.loc["2003"]

        self.assertTrue(bool(row["event_candidate_flag"]))
        self.assertEqual(row["candidate_bucket"], "post_earnings_follow_candidate")
        self.assertEqual(row["candidate_bucket_label"], "決算通過後候補")
        self.assertIn("決算通過後", row["event_caution_reason"])

    def test_buy_candidate_storage_json_is_finite_and_compact(self):
        tables = _build_tables()
        bundle = {
            "meta": {"mode": "1530", "generated_at": "2026-04-24T06:30:00+00:00"},
            "swing_candidates_1w": tables["1w"],
            "swing_candidates_1m": tables["1m"],
            "swing_candidates_3m": tables["3m"],
            "swing_1w_candidates_audit": tables["audit_1w"],
            "swing_1m_candidates_audit": tables["audit_1m"],
            "swing_3m_candidates_audit": tables["audit_3m"],
            "diagnostics": {"base_meta": {"edinetdb_calendar_status": "ok"}},
        }
        storage_bundle = app._bundle_for_storage(bundle)
        text = app.bundle_to_json_text(storage_bundle)
        payload = json.loads(text)

        def walk(value):
            if isinstance(value, float):
                self.assertTrue(math.isfinite(value))
            elif isinstance(value, dict):
                for item in value.values():
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)
        for key in ["swing_candidates_1w", "swing_candidates_1m", "swing_candidates_3m"]:
            components = payload[key][0].get("score_components", {})
            self.assertIsInstance(components, dict)
            self.assertLessEqual(len(components), 16)

    def test_entry_decision_includes_compact_cautions_without_duplicates(self):
        row = pd.Series(
            {
                "entry_stance_label": "押し待ち",
                "entry_caution": "決算近い / 20日線乖離大",
                "candidate_bucket_label": "追いかけ注意",
                "event_caution_reason": "20日線乖離大。追いかけ注意",
                "fallback_used": False,
            }
        )

        text = app._format_candidate_entry_decision(row)

        self.assertEqual(text, "押し待ち\n注意: 決算近い / 20日線乖離大 / 追いかけ注意")
        self.assertEqual(text.count("20日線乖離大"), 1)
        self.assertEqual(text.count("追いかけ注意"), 1)

    def test_entry_decision_keeps_normal_candidate_single_line(self):
        row = pd.Series(
            {
                "entry_stance_label": "長期主導で買い検討",
                "entry_caution": "",
                "candidate_bucket_label": "通常候補",
                "event_caution_reason": "",
                "fallback_used": False,
            }
        )

        self.assertEqual(app._format_candidate_entry_decision(row), "長期主導で買い検討")

    def test_entry_decision_marks_fallback_candidate(self):
        row = pd.Series(
            {
                "entry_stance_label": "補完・監視",
                "entry_caution": "",
                "candidate_bucket_label": "通常候補",
                "event_caution_reason": "",
                "fallback_used": True,
            }
        )

        self.assertEqual(app._format_candidate_entry_decision(row), "補完・監視\n注意: 補完候補")

    def test_candidate_basis_is_not_truncated_and_nikkei_link_is_kept(self):
        long_reason = " / ".join(
            [
                "3か月の強さが続いている",
                "直近1か月も大きく崩れていない",
                "流動性がある",
                "3か月トレンド良好",
                "業種主導性が残っている",
            ]
        )
        frame = pd.DataFrame(
            [
                {
                    "candidate_rank_3m": 1,
                    "sector_name": "電気機器",
                    "code": "6920",
                    "name": "レーザーテック",
                    "entry_stance_label": "押し待ち",
                    "selection_reason": long_reason,
                    "horizon_fit_reason": "業種主導性 / 中期資金継続 / 3か月トレンド良好",
                    "entry_caution": "決算近い / 20日線乖離大",
                    "candidate_bucket_label": "追いかけ注意",
                    "event_caution_reason": "20日線乖離大。追いかけ注意",
                    "fallback_used": False,
                    "current_price": "100",
                    "live_ret_vs_prev_close": "1.2",
                    "earnings_announcement_date": "2026-04-30",
                    "nikkei_search": "https://www.nikkei.com/search?keyword=レーザーテック",
                }
            ]
        )

        view, reason, note = app._build_candidate_focus_view(
            frame,
            rank_col="candidate_rank_3m",
            sector_rank_lookup={"電気機器": 1},
            center_reference_map={},
            scope_label="3か月軸",
        )

        self.assertEqual(reason, "")
        self.assertEqual(note, "")
        row = view.iloc[0]
        self.assertIn("注意: 決算近い / 20日線乖離大 / 追いかけ注意", row["entry_stance_label"])
        self.assertIn(long_reason, row["candidate_basis"])
        self.assertNotIn("…", row["candidate_basis"])
        self.assertEqual(row["nikkei_search"], "https://www.nikkei.com/search?keyword=レーザーテック")

    def test_candidate_table_links_only_http_urls(self):
        html = app._candidate_table_cell_html("https://www.nikkei.com/search?keyword=5801", column_label="日経リンク")

        self.assertIn('href="https://www.nikkei.com/search?keyword=5801"', html)
        self.assertIn("日経リンク", html)
        self.assertEqual(app._candidate_table_cell_html("javascript:alert(1)", column_label="日経リンク"), "")

    def test_persistence_representative_focus_keeps_reason_without_nikkei_link(self):
        frame = pd.DataFrame(
            [
                {
                    "sector_name": "電気機器",
                    "representative_stocks": [
                        {
                            "code": "6920",
                            "name": "レーザーテック",
                            "center_note": "3か月の中心銘柄",
                            "earnings_announcement_date": "2026-04-30",
                            "nikkei_search": "https://www.nikkei.com/search?keyword=6920",
                        }
                    ],
                    "core_representatives_reason": "strong_3m",
                }
            ]
        )

        view = app._build_center_stock_focus_view(
            frame,
            sector_rank_lookup={"電気機器": 1},
            timeframe="3m",
        )

        self.assertEqual(
            list(view.columns),
            ["sector_name", "code", "name", "center_note", "earnings_announcement_date"],
        )
        self.assertEqual(view.iloc[0]["center_note"], "3か月の中心銘柄")
        self.assertNotIn("nikkei_search", view.columns)

    def test_today_representative_focus_keeps_quality_and_omits_nikkei_link(self):
        representative_frame = pd.DataFrame(
            [
                {
                    "today_rank": 1,
                    "representative_rank": 1,
                    "sector_name": "卸売業",
                    "code": "2737",
                    "name": "トーメンデバイス",
                    "live_ret_vs_prev_close": 5.0,
                    "current_price": 1000,
                    "live_turnover_value": 100000000,
                    "representative_selected_reason": "material_supported_breakout",
                    "representative_quality_flag": "本日決算注意",
                    "representative_fallback_reason": "",
                    "earnings_announcement_date": "2026-04-25",
                    "nikkei_search": "https://www.nikkei.com/search?keyword=2737",
                }
            ]
        )

        view = app._build_center_stock_focus_view(
            pd.DataFrame([{"sector_name": "卸売業"}]),
            sector_rank_lookup={"卸売業": 1},
            timeframe="today",
            representative_frame=representative_frame,
        )

        self.assertIn("representative_selected_reason", view.columns)
        self.assertIn("representative_quality_flag", view.columns)
        self.assertNotIn("nikkei_search", view.columns)


if __name__ == "__main__":
    unittest.main()
