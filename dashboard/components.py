import streamlit as st
import pandas as pd


def render_match_card(match: pd.Series, innings: pd.DataFrame):
    """Render a single match card with score."""

    # Status color
    status = match["status"] or "Scheduled"
    if "progress" in status.lower() or "live" in status.lower():
        status_color = "🟢"
    elif "final" in status.lower() or "result" in status.lower():
        status_color = "🏁"
    else:
        status_color = "🕐"

    with st.container():
        st.markdown(f"### {status_color} {match['name']}")
        st.caption(f"📍 {match['venue']} &nbsp;|&nbsp; 📅 {match['match_date']}")

        if not innings.empty:
            cols = st.columns(len(innings))
            for col, (_, inning) in zip(cols, innings.iterrows()):
                with col:
                    st.metric(
                        label=inning["inning_name"],
                        value=f"{inning['runs']}/{inning['wickets']}",
                        delta=f"{inning['overs']} ov"
                    )
        else:
            st.info("Scorecard not yet available.")

        if match["winner"]:
            st.success(f"🏆 {match['winner']} won")
        elif "toss" in status.lower() or match["toss_winner"]:
            st.info(f"🪙 Toss: {match['toss_winner']} won the toss")

        st.divider()


def render_scorecard_table(df: pd.DataFrame, role: str):
    """Render batting or bowling scorecard as a styled table."""
    if df.empty:
        st.info(f"No {role} data available.")
        return

    if role == "batting":
        display = df[[
            "player_name", "team", "runs",
            "balls_faced", "fours", "sixes", "strike_rate"
        ]].rename(columns={
            "player_name": "Player",
            "team":        "Team",
            "runs":        "R",
            "balls_faced": "B",
            "fours":       "4s",
            "sixes":       "6s",
            "strike_rate": "SR",
        })
    else:
        display = df[[
            "player_name", "team", "wickets",
            "overs_bowled", "economy"
        ]].rename(columns={
            "player_name":  "Player",
            "team":         "Team",
            "wickets":      "W",
            "overs_bowled": "Ov",
            "economy":      "Eco",
        })

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
    )


def render_h2h_card(team1: str, team2: str, h2h: dict):
    """Render head to head summary card."""
    st.markdown("#### Head to Head")
    col1, col_mid, col2 = st.columns([2, 1, 2])

    with col1:
        st.metric(team1, f"{h2h['team1_wins']} wins")
        st.progress(h2h["team1_wr"] / 100)

    with col_mid:
        st.markdown(
            f"<div style='text-align:center;padding-top:20px'>"
            f"<b>{h2h['total']}</b><br>matches</div>",
            unsafe_allow_html=True
        )

    with col2:
        st.metric(team2, f"{h2h['team2_wins']} wins")
        st.progress(h2h["team2_wr"] / 100)


import plotly.graph_objects as go


def render_win_probability_gauge(
    team1: str,
    team2: str,
    team1_prob: float,
    team2_prob: float,
):
    """Render a Plotly gauge showing win probability for both teams."""

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=team1_prob,
        title={"text": f"{team1} win probability"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar":  {"color": "steelblue"},
            "steps": [
                {"range": [0,  40],  "color": "salmon"},
                {"range": [40, 60],  "color": "lightyellow"},
                {"range": [60, 100], "color": "lightgreen"},
            ],
            "threshold": {
                "line":  {"color": "black", "width": 3},
                "thickness": 0.75,
                "value": 50,
            },
        },
        number={"suffix": "%"},
    ))

    fig.update_layout(
        height=280,
        margin=dict(t=40, b=10, l=20, r=20),
    )

    col1, col2, col3 = st.columns([2, 3, 2])

    with col1:
        st.metric(
            label=team1,
            value=f"{team1_prob}%",
            delta=f"{team1_prob - 50:.1f}% vs even"
        )

    with col2:
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.metric(
            label=team2,
            value=f"{team2_prob}%",
            delta=f"{team2_prob - 50:.1f}% vs even"
        )


def render_batting_chart(batters: pd.DataFrame):
    """Bar chart of top batters by average runs."""
    if batters.empty:
        st.info("No batting data available.")
        return

    fig = go.Figure()

    colors = ["steelblue" if i % 2 == 0 else "cornflowerblue"
              for i in range(len(batters))]

    fig.add_trace(go.Bar(
        x=batters["player_name"],
        y=batters["avg_runs"],
        marker_color=colors,
        text=batters["avg_runs"],
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Avg runs: %{y}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title="Top batters — average runs",
        xaxis_tickangle=-30,
        yaxis_title="Average runs",
        height=350,
        margin=dict(t=50, b=80, l=40, r=20),
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_bowling_chart(bowlers: pd.DataFrame):
    """Scatter chart of bowlers — wickets vs economy."""
    if bowlers.empty:
        st.info("No bowling data available.")
        return

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=bowlers["avg_economy"],
        y=bowlers["avg_wickets"],
        mode="markers+text",
        text=bowlers["player_name"],
        textposition="top center",
        marker=dict(
            size=12,
            color=bowlers["total_wickets"],
            colorscale="Blues",
            showscale=True,
            colorbar=dict(title="Total wickets"),
        ),
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Avg wickets: %{y}<br>"
            "Avg economy: %{x}<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        title="Bowlers — wickets vs economy (bigger = more total wickets)",
        xaxis_title="Average economy (lower is better →)",
        yaxis_title="Average wickets per match",
        height=380,
        margin=dict(t=50, b=50, l=50, r=20),
    )

    # Add quadrant lines
    fig.add_hline(y=1.0,  line_dash="dot", line_color="gray", opacity=0.4)
    fig.add_vline(x=8.0,  line_dash="dot", line_color="gray", opacity=0.4)

    st.plotly_chart(fig, use_container_width=True)


def render_radar_chart(player_name: str, stats: dict):
    """Radar chart for a single player's stats."""
    categories = ["Runs", "Strike Rate", "Fours", "Sixes", "Innings"]
    values     = [
        stats.get("avg_runs",  0),
        stats.get("avg_sr",    0) / 2,  # scale to similar range
        stats.get("total_fours", 0),
        stats.get("total_sixes", 0) * 3,
        stats.get("innings",   0) * 5,
    ]

    fig = go.Figure(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill="toself",
        fillcolor="rgba(70,130,180,0.2)",
        line=dict(color="steelblue", width=2),
    ))

    fig.update_layout(
        title=player_name,
        polar=dict(
            radialaxis=dict(visible=True, showticklabels=False)
        ),
        height=300,
        margin=dict(t=50, b=20, l=20, r=20),
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_ai_insight_card(
    insight: str,
    insight_type: str = "pre",
):
    """Render the LLM analyst commentary card."""

    icons = {
        "pre":  "🔮 Pre-match analysis",
        "live": "⚡ Live analyst update",
        "post": "📊 Post-match summary",
    }

    title = icons.get(insight_type, "🤖 AI Analyst")

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-left: 4px solid #4a9eff;
        border-radius: 8px;
        padding: 20px 24px;
        margin: 8px 0;
    ">
        <p style="color:#4a9eff; font-size:13px; margin:0 0 8px 0;
                  font-weight:600; letter-spacing:1px; text-transform:uppercase;">
            {title}
        </p>
        <p style="color:#e8e8e8; font-size:15px; line-height:1.7; margin:0;">
            {insight}
        </p>
        <p style="color:#666; font-size:11px; margin:8px 0 0 0;">
            Powered by Gemini 2.5 Flash · grounded in match statistics
        </p>
    </div>
    """, unsafe_allow_html=True)


def render_player_spotlight_card(spotlight: str):
    """Render the player spotlight card."""
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1a2e1a 0%, #162e16 100%);
        border-left: 4px solid #4aff7a;
        border-radius: 8px;
        padding: 20px 24px;
        margin: 8px 0;
    ">
        <p style="color:#4aff7a; font-size:13px; margin:0 0 8px 0;
                  font-weight:600; letter-spacing:1px; text-transform:uppercase;">
            ⭐ Players to watch
        </p>
        <p style="color:#e8e8e8; font-size:15px; line-height:1.7; margin:0;">
            {spotlight}
        </p>
        <p style="color:#666; font-size:11px; margin:8px 0 0 0;">
            Based on rolling form over last 5 matches
        </p>
    </div>
    """, unsafe_allow_html=True)