import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import time
from datetime import date
from dashboard.data_loader import (
    get_live_matches,
    get_match_innings,
    get_match_batting,
    get_match_bowling,
    get_top_batters,
    get_top_bowlers,
    get_h2h_summary,
)
from dashboard.components import (
    render_match_card,
    render_scorecard_table,
    render_h2h_card,
    render_win_probability_gauge,
    render_batting_chart,
    render_bowling_chart,
    render_radar_chart,
    render_ai_insight_card,
    render_player_spotlight_card,
)
from models.predictor import predict_win_probability
from llm.insight_generator import get_pre_match_insight, get_live_insight
from llm.player_spotlight import generate_player_spotlight
from processing.team_features import get_team_form_at_date
from processing.h2h_features import get_h2h_at_date, get_venue_stats

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPL Analytics Dashboard",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏏 IPL Analytics")
    st.caption("Live data · ML predictions · AI insights")
    st.divider()

    auto_refresh = st.toggle("Auto refresh (60s)", value=True)
    if st.button("🔄 Refresh now"):
        st.rerun()

    st.divider()
    st.caption("📡 Data: ESPN Cricinfo")
    st.caption("🤖 Model: XGBoost")
    st.caption("💬 Insights: Gemini 2.5 Flash")

# ── Load matches ─────────────────────────────────────────────────────
matches = get_live_matches()

if matches.empty:
    st.warning("""
        No match data found.
        Make sure `python main.py` is running in another terminal.
    """)
    st.stop()

# ── Match selector ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Select match")
    selected_name = st.selectbox(
        "Match",
        matches["name"].tolist(),
        label_visibility="collapsed"
    )

selected  = matches[matches["name"] == selected_name].iloc[0]
match_id  = selected["match_id"]
team1     = selected["team1"]
team2     = selected["team2"]
venue     = selected["venue"] or "TBC"
match_date = str(selected["match_date"])
status    = selected["status"] or ""
toss_winner = selected["toss_winner"] or team1

# ── Title ────────────────────────────────────────────────────────────
st.title("🏏 IPL 2025 — Live Analytics Dashboard")
st.caption(f"Showing: **{selected_name}** · {venue} · {match_date}")
st.divider()

# ── Section 1: Live scores ───────────────────────────────────────────
st.subheader("📡 Live & Recent Matches")
for _, match in matches.iterrows():
    innings = get_match_innings(match["match_id"])
    render_match_card(match, innings)

st.divider()

# ── Section 2: Win probability ───────────────────────────────────────
st.subheader(f"📊 Win Probability — {team1} vs {team2}")

pred = None 

try:
    pred = predict_win_probability(
        team1=team1,
        team2=team2,
        venue=venue,
        toss_winner=toss_winner,
        date=match_date,
    )
    render_win_probability_gauge(
        team1=team1,
        team2=team2,
        team1_prob=pred["team1_win_prob"],
        team2_prob=pred["team2_win_prob"],
    )
except Exception as e:
    st.warning(f"Win probability unavailable: {e}")

st.divider()

# ── Section 3: Scorecard ─────────────────────────────────────────────
st.subheader(f"📋 Scorecard — {selected_name}")
tab_bat, tab_bowl, tab_h2h = st.tabs([
    "🏏 Batting", "🎳 Bowling", "⚔️ Head to Head"
])

with tab_bat:
    batting = get_match_batting(match_id)
    render_scorecard_table(batting, "batting")

with tab_bowl:
    bowling = get_match_bowling(match_id)
    render_scorecard_table(bowling, "bowling")

with tab_h2h:
    h2h = get_h2h_summary(team1, team2)
    render_h2h_card(team1, team2, h2h)

st.divider()

# ── Section 4: Player performance ───────────────────────────────────
st.subheader(f"👥 Player Performance — {team1} vs {team2}")

batters = get_top_batters(team1, team2)
bowlers = get_top_bowlers(team1, team2)

col_bat, col_bowl = st.columns(2)
with col_bat:
    render_batting_chart(batters)

with col_bowl:
    render_bowling_chart(bowlers)

# Radar charts for top 2 batters
if not batters.empty:
    st.markdown("#### Player radar charts")
    radar_cols = st.columns(min(2, len(batters)))
    for i, (col, (_, player)) in enumerate(
        zip(radar_cols, batters.head(2).iterrows())
    ):
        with col:
            render_radar_chart(
                player_name=player["player_name"],
                stats={
                    "avg_runs":    player["avg_runs"],
                    "avg_sr":      player["avg_sr"],
                    "total_fours": player["total_fours"],
                    "total_sixes": player["total_sixes"],
                    "innings":     player["innings"],
                }
            )

st.divider()

# ── Section 5: AI analyst ────────────────────────────────────────────
st.subheader("🤖 AI Analyst")

try:
    form        = get_team_form_at_date(match_date)
    venue_stats = get_venue_stats()
    h2h_stats   = get_h2h_at_date(team1, team2, match_date)

    def form_val(team, field):
        if form.empty: return 0.5
        row = form[form["team"] == team]
        return float(row[field].iloc[0]) if not row.empty else 0.5

    def venue_wr(team):
        if venue_stats.empty: return 0.5
        row = venue_stats[
            (venue_stats["team"] == team) &
            (venue_stats["venue"] == venue)
        ]
        return float(row["venue_win_rate"].iloc[0]) if not row.empty else 0.5

    insight_type = (
        "live" if "progress" in status.lower() or "live" in status.lower()
        else "post" if selected["winner"]
        else "pre"
    )

    # Guard: only use pred if win probability was successfully computed
    if pred is None:
        insight = "Win probability model not ready yet — run main.py first."

    elif insight_type == "live":
        innings_data = get_match_innings(match_id)
        if not innings_data.empty:
            latest = innings_data.iloc[-1]
            insight = get_live_insight(
                team1=team1,
                team2=team2,
                team1_win_prob=pred["team1_win_prob"],
                team2_win_prob=pred["team2_win_prob"],
                batting_team=latest["inning_name"],
                runs=int(latest["runs"]),
                wickets=int(latest["wickets"]),
                overs=float(latest["overs"]),
            )
        else:
            insight = "Live insight unavailable — no innings data yet."

    else:
        insight = get_pre_match_insight(
            team1=team1,
            team2=team2,
            venue=venue,
            team1_win_prob=pred["team1_win_prob"],
            team2_win_prob=pred["team2_win_prob"],
            team1_win_rate=form_val(team1, "rolling_win_rate"),
            team2_win_rate=form_val(team2, "rolling_win_rate"),
            team1_streak=int(form_val(team1, "streak")),
            team2_streak=int(form_val(team2, "streak")),
            team1_h2h_win_rate=h2h_stats["team_a_win_rate"],
            h2h_total=h2h_stats["h2h_total"],
            team1_venue_win_rate=venue_wr(team1),
            team2_venue_win_rate=venue_wr(team2),
            toss_winner=toss_winner,
        )

    render_ai_insight_card(insight, insight_type)

except Exception as e:
    st.warning(f"AI insight unavailable: {e}")

# Player spotlight
try:
    spotlight = generate_player_spotlight(
        team1=team1,
        team2=team2,
        date=match_date,
    )
    render_player_spotlight_card(spotlight)
except Exception as e:
    st.warning(f"Player spotlight unavailable: {e}")

# ── Auto refresh ─────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(60)
    st.rerun()