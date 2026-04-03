import pandas as pd
import plotly.express as px
import streamlit as st

from pathlib import Path

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="Formula 1 Race Performance Dashboard",
    page_icon="🏎️",
    layout="wide",
)

# -----------------------------
# Styling
# -----------------------------
st.markdown(
    """
    <style>
    button[data-baseweb="tab"] {
        font-size: 1.15rem;
        font-weight: 700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# -----------------------------
# Data loading
# -----------------------------

BASE_DIR = Path(__file__).parent
DRIVER_RACE_PATH = BASE_DIR / "data" / "processed" /"driver_race_metrics.parquet"
OVERTAKES_PATH = BASE_DIR / "data" / "processed" / "overtakes_by_race.parquet"
TARGET_SEASONS = [2020, 2021, 2022, 2023, 2024, 2025]


@st.cache_data
def load_driver_race_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    numeric_cols = [
        "season",
        "round",
        "GridPosition",
        "FinishPosition",
        "Points",
        "position_delta",
        "normalized_delta",
        "relative_pace",
        "lap_time_std",
        "consistency_index",
        "stint_count",
        "had_disruption",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data
def load_overtakes_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()

    numeric_cols = ["season", "round", "drivers_count", "total_overtakes_est"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


try:
    df = load_driver_race_data(DRIVER_RACE_PATH)
    overtakes = load_overtakes_data(OVERTAKES_PATH)
except Exception as exc:
    st.error(f"Failed to load data: {exc}")
    st.stop()


# -----------------------------
# Season availability / coverage
# -----------------------------
driver_seasons = set(df["season"].dropna().astype(int).unique())
overtake_seasons = set(overtakes["season"].dropna().astype(int).unique())

driver_available = [s for s in TARGET_SEASONS if s in driver_seasons]
common_available = [s for s in TARGET_SEASONS if s in driver_seasons and s in overtake_seasons]

if not driver_available:
    st.error("No target seasons found in driver race dataset.")
    st.stop()

# Prefer seasons that exist in both datasets so all tabs work cleanly.
available_seasons = common_available if common_available else driver_available
latest_season = max(available_seasons)


# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.title("Controls")
season = st.sidebar.selectbox(
    "Season",
    options=available_seasons,
    index=available_seasons.index(latest_season),
)



# -----------------------------
# Season filtering + shared cleaned data
# -----------------------------
df_s = df[df["season"] == season].copy()

# Classified drivers only
if "FinishPosition" in df_s.columns:
    df_s = df_s[df_s["FinishPosition"].notna()].copy()

# Valid grid positions for qualifying-based charts
# GridPosition == 0 usually means pit-lane start / non-standard grid slot.
df_grid = df_s[df_s["GridPosition"].between(1, 20)].copy()

# Keep a version for team/driver racecraft charts
# position_delta is still meaningful for classified drivers.
df_racecraft = df_s.copy()

# Overtakes for selected season
over_s = overtakes[overtakes["season"] == season].copy()

# Merge overtakes back to race-level driver data so overview/track sections can use it.
over_lookup = over_s[["season", "round", "total_overtakes_est"]].drop_duplicates()
df_joined = df_s.merge(over_lookup, on=["season", "round"], how="left")


# -----------------------------
# Helpers
# -----------------------------
def section_header(title: str, description: str | None = None) -> None:
    st.subheader(title)
    if description:
        st.caption(description)


def insight_box(text: str) -> None:
    st.markdown(f"**What to look for:** {text}")


def outcome_box(text: str) -> None:
    st.markdown(f"**How this relates to race outcome:** {text}")


def methodology_box(title: str, text: str) -> None:
    st.markdown(f"**{title}**")
    st.markdown(text)


def key_insight_subsection(title: str, text: str) -> None:
    st.markdown(f"##### {title}")
    st.info(text)

def make_driver_abbrev(full_name: str) -> str:
    if not isinstance(full_name, str) or not full_name.strip():
        return "—"

    special_map = {
        "Max Verstappen": "VER",
        "Lando Norris": "NOR",
        "Lewis Hamilton": "HAM",
        "Charles Leclerc": "LEC",
        "Carlos Sainz": "SAI",
        "George Russell": "RUS",
        "Sergio Perez": "PER",
        "Oscar Piastri": "PIA",
        "Fernando Alonso": "ALO",
        "Lance Stroll": "STR",
        "Pierre Gasly": "GAS",
        "Esteban Ocon": "OCO",
        "Yuki Tsunoda": "TSU",
        "Alexander Albon": "ALB",
        "Alex Albon": "ALB",
        "Logan Sargeant": "SAR",
        "Nico Hulkenberg": "HUL",
        "Nico Hülkenberg": "HUL",
        "Kevin Magnussen": "MAG",
        "Valtteri Bottas": "BOT",
        "Zhou Guanyu": "ZHO",
        "Daniel Ricciardo": "RIC",
        "Liam Lawson": "LAW",
        "Oliver Bearman": "BEA",
        "Andrea Kimi Antonelli": "ANT",
        "Kimi Antonelli": "ANT",
        "Franco Colapinto": "COL",
        "Jack Doohan": "DOO",
        "Gabriel Bortoleto": "BOR",
        "Isack Hadjar": "HAD",
    }
    if full_name in special_map:
        return special_map[full_name]

    surname = full_name.strip().split()[-1]
    return surname[:3].upper()


def make_team_abbrev(team_name: str) -> str:
    if not isinstance(team_name, str) or not team_name.strip():
        return "—"

    special_map = {
        "Mercedes": "MER",
        "Red Bull": "RBR",
        "Red Bull Racing": "RBR",
        "McLaren": "MCL",
        "Ferrari": "FER",
        "Aston Martin": "AMR",
        "Aston Martin Aramco": "AMR",
        "Alpine": "ALP",
        "RB": "RB",
        "Racing Bulls": "RB",
        "Visa Cash App RB": "RB",
        "AlphaTauri": "AT",
        "Williams": "WIL",
        "Kick Sauber": "SAU",
        "Sauber": "SAU",
        "Alfa Romeo": "ALR",
        "Haas": "HAA",
    }
    if team_name in special_map:
        return special_map[team_name]

    letters = ''.join(ch for ch in team_name if ch.isalpha()).upper()
    return letters[:3] if letters else "—"


def get_world_champions(frame: pd.DataFrame) -> tuple[str, str]:
    if frame.empty or "Points" not in frame.columns:
        return "—", "—"

    driver_champ = "—"
    team_champ = "—"

    if "FullName" in frame.columns:
        driver_points = (
            frame.groupby("FullName", as_index=False)["Points"]
            .sum()
            .sort_values(["Points", "FullName"], ascending=[False, True])
        )
        if not driver_points.empty:
            driver_champ = make_driver_abbrev(driver_points.iloc[0]["FullName"])

    if "TeamName" in frame.columns:
        team_points = (
            frame.groupby("TeamName", as_index=False)["Points"]
            .sum()
            .sort_values(["Points", "TeamName"], ascending=[False, True])
        )
        if not team_points.empty:
            team_champ = make_team_abbrev(team_points.iloc[0]["TeamName"])

    return driver_champ, team_champ


def metric_card_row() -> None:
    driver_champ, team_champ = get_world_champions(df_s)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Season", season)
    with col2:
        st.metric("Races", int(df_s["round"].nunique()) if "round" in df_s.columns else 0)
    with col3:
        st.metric("World Drivers' Champion", driver_champ)
    with col4:
        st.metric("World Constructors' Champion", team_champ)


def safe_spearman(frame: pd.DataFrame, x_col: str, y_col: str) -> float | None:
    tmp = frame[[x_col, y_col]].dropna()
    if len(tmp) < 2:
        return None
    return tmp[x_col].corr(tmp[y_col], method="spearman")


# -----------------------------
# App title / intro
# -----------------------------
st.title("🏎️ Formula 1 Race Performance Dashboard")
st.markdown(
    """
This dashboard explores how Formula 1 race outcomes are shaped by **qualifying position**,
**driver racecraft**, **track overtaking difficulty**, and **team race-day execution**.
"""
)
metric_card_row()

missing_driver_target = [s for s in TARGET_SEASONS if s not in driver_seasons]
missing_overtake_target = [s for s in TARGET_SEASONS if s not in overtake_seasons]

if missing_driver_target or missing_overtake_target:
    msg_parts = []
    if missing_driver_target:
        msg_parts.append(f"driver-race data missing: {missing_driver_target}")
    if missing_overtake_target:
        msg_parts.append(f"overtake data missing: {missing_overtake_target}")
    st.warning("Season coverage note — " + " | ".join(msg_parts))

st.divider()

# Top-level structure: Overview -> Drivers -> Teams -> Tracks
# This makes the app easier to navigate than one long scrolling page.
tab_overview, tab_drivers, tab_teams, tab_tracks = st.tabs([
    "Overview",
    "Drivers",
    "Teams",
    "Tracks",
])


# =============================
# OVERVIEW TAB
# =============================
with tab_overview:
    with st.container():
        section_header(
            "Important Factors in Race Outcome",
            "This chart summarizes which race-level and driver-level factors have the strongest monotonic relationship with better race outcomes after aligning factor directions so higher values consistently mean better performance.",
        )
        insight_box("Bars farther to the right are more strongly associated with better finishes. Bars to the left are associated with worse finishes.")
        outcome_box("This provides a high-level ranking of what matters most on race day: qualifying, pace, racecraft, strategy proxies, and track-specific overtaking environment.")

        factor_df = df_joined.copy()
        factor_df["overtakes"] = factor_df["total_overtakes_est"]
            
        if "event_name" in factor_df.columns:
            factor_df["fastest_lap_proxy"] = 0

            valid_pace_df = factor_df.dropna(subset=["relative_pace"]).copy()

            if not valid_pace_df.empty:
                fastest_lap_winners = (
                    valid_pace_df.groupby(["season", "round"])["relative_pace"]
                    .idxmin()
                    .tolist()
                )
                factor_df.loc[fastest_lap_winners, "fastest_lap_proxy"] = 1
        else:
            factor_df["fastest_lap_proxy"] = 0

        # Align directions so higher values consistently mean better performance.
        factor_df["better_finish_outcome"] = -factor_df["FinishPosition"]
        factor_df["better_grid_position"] = -factor_df["GridPosition"]
        factor_df["faster_relative_pace"] = -factor_df["relative_pace"]

        # Pit stop time is not currently available in the processed parquet.
        # We keep the label out of the chart rather than show a misleading empty bar.
        corr_candidates = {
            "Better grid position": "better_grid_position",
            "Faster relative pace": "faster_relative_pace",
            "Positions gained": "position_delta",
            "Lap time variability": "lap_time_std",
            "Consistency index": "consistency_index",
            "Estimated overtakes": "overtakes",
            "Fastest lap (proxy)": "fastest_lap_proxy",
        }

        corr_rows = []
        for label, col in corr_candidates.items():
            if col in factor_df.columns:
                corr_val = safe_spearman(factor_df, col, "better_finish_outcome")
                if corr_val is not None:
                    corr_rows.append({"factor": label, "correlation": corr_val})

        corr_df = pd.DataFrame(corr_rows).sort_values("correlation", ascending=True)

        key_insight_text = None
        if not corr_df.empty:
            corr_df["abs_correlation"] = corr_df["correlation"].abs()
            strongest = corr_df.sort_values("abs_correlation", ascending=False).head(3).copy()
            # Build list like: "Factor (0.80)"
            factors_list = [
                f"**{row['factor']} ({row['correlation']:.2f})**"
                for _, row in strongest.iterrows()
            ]
            # Format nicely with commas + "and"
            if len(factors_list) == 1:
                factors_text = factors_list[0]
            elif len(factors_list) == 2:
                factors_text = " and ".join(factors_list)
            else:
                factors_text = ", ".join(factors_list[:-1]) + f", and {factors_list[-1]}"

            key_insight_text = (
                f"For this season, some of the strongest signals associated with better finishes are: {factors_text}. "
                "Overall, the chart is direction-aligned so positive correlations consistently indicate factors associated with stronger race outcomes."
            )

        fig_corr = px.bar(
            corr_df,
            x="correlation",
            y="factor",
            orientation="h",
            title=f"Important Factors in Race Outcome ({season})",
            labels={"correlation": "Spearman Correlation with Better (Lower) Finish Position", "factor": "Factor"},
        )
        fig_corr.add_vline(x=0, line_dash="dash")
        st.plotly_chart(fig_corr, use_container_width=True)

        if key_insight_text:
            key_insight_subsection("Key Insight", key_insight_text)

    st.divider()

    with st.container():
        section_header(
            "Qualifying vs Race Outcome",
            "Qualifying strongly shapes race outcomes, but drivers still gain and lose positions through pace, strategy, and race incidents.",
        )
        insight_box("A tight diagonal pattern means starting position heavily influences where drivers finish. The spread around the diagonal shows where race-day variation still matters.")
        outcome_box("Qualifying creates the starting advantage. Drivers and teams that outperform their starting position are overcoming one of the strongest structural constraints in F1.")

        col1, col2 = st.columns([1.25, 1])

        with col1:
            scatter_df = df_grid.copy()

            fig_scatter = px.scatter(
                scatter_df,
                x="GridPosition",
                y="FinishPosition",
                hover_data=["FullName", "TeamName", "season", "round"],
                title=f"Grid Position vs Finish Position ({season})",
                opacity=0.6,
                labels={"GridPosition": "Grid Position", "FinishPosition": "Finish Position"},
            )

            fig_scatter.update_yaxes(autorange="reversed")
            fig_scatter.update_xaxes(range=[0.5, 20.5])
            fig_scatter.update_yaxes(range=[21, 0])
            fig_scatter.add_trace(px.line(x=list(range(1, 21)), y=list(range(1, 21))).data[0])

            if len(scatter_df) > 1:
                corr = scatter_df["GridPosition"].corr(scatter_df["FinishPosition"], method="spearman")
                fig_scatter.update_layout(
                    title=f"Grid Position vs Finish Position ({season}) | Spearman ρ = {corr:.2f}"
                )

            st.plotly_chart(fig_scatter, use_container_width=True)

        with col2:
            team_grid_finish = (
                df_grid.groupby("TeamName", as_index=False)
                .agg(
                    avg_grid=("GridPosition", "mean"),
                    avg_finish=("FinishPosition", "mean"),
                )
                .sort_values("avg_grid")
            )

            team_grid_finish_long = team_grid_finish.melt(
                id_vars="TeamName",
                value_vars=["avg_grid", "avg_finish"],
                var_name="metric",
                value_name="position",
            )

            fig_grouped = px.bar(
                team_grid_finish_long,
                x="TeamName",
                y="position",
                color="metric",
                barmode="group",
                title=f"Average Grid vs Finish Position by Team ({season})",
                labels={"position": "Position", "metric": "Metric", "TeamName": "Team Name"},
            )
            # overriding legend labels via traces
            fig_grouped.update_traces(
                selector=dict(name="avg_grid"),
                name="Avg Grid Position"
            )
            fig_grouped.update_traces(
                selector=dict(name="avg_finish"),
                name="Avg Finish Position"
            )

            fig_grouped.update_yaxes(autorange="reversed")
            st.plotly_chart(fig_grouped, use_container_width=True)

    st.divider()

    methodology_box(
    "Methodology notes",
        """
        - The factor-importance chart uses **Spearman correlation with better finish outcome**, implemented as `-FinishPosition` so higher values mean better results.
        - `Better grid position` is implemented as `-GridPosition`, and `Faster relative pace` is implemented as `-relative_pace`, so higher values consistently mean better performance.
        - `Estimated overtakes` is a race-level proxy merged back to each driver-race row.
        - `Fastest lap (proxy)` is approximated by marking the driver with the lowest average relative pace in each race; it is not the official FIA fastest-lap award.
        """,
        )


# =============================
# DRIVERS TAB
# =============================
with tab_drivers:
    with st.container():
        section_header(
            "Drivers Who Gain the Most Positions",
            "This chart highlights drivers who consistently move forward during races. It captures racecraft, tire management, and race execution rather than pure qualifying speed.",
        )
        insight_box("Drivers near the top of this chart regularly outperform their starting position on Sundays.")
        outcome_box("When drivers repeatedly gain positions, they are converting race pace, tire management, and overtaking opportunities into better finishing results.")

        driver_gain = (
            df_racecraft.groupby("FullName", as_index=False)
            .agg(
                races=("round", "count"),
                total_gain=("position_delta", "sum"),
                avg_gain=("position_delta", "mean"),
                TeamName=("TeamName", "last"),
            )
        )

        MIN_RACES_DRIVER = 10
        driver_gain = driver_gain[driver_gain["races"] >= MIN_RACES_DRIVER].copy()
        driver_gain = driver_gain.sort_values("total_gain", ascending=False)

        fig_driver_gain = px.bar(
            driver_gain.head(10),
            x="total_gain",
            y="FullName",
            orientation="h",
            color="TeamName",
            hover_data=["races", "avg_gain"],
            title=f"Top Drivers by Positions Gained ({season})",
            labels={"FullName": "Full Name", "total_gain": "Total Gain"},
        )
        fig_driver_gain.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_driver_gain, use_container_width=True)

    st.divider()

    with st.container():
        section_header(
            "Which Drivers Combine Pace and Racecraft?",
            "Drivers are mapped by average race pace and average positions gained. This separates raw pace from racecraft and highlights different performance archetypes.",
        )
        insight_box("Top-left drivers combine strong pace with strong racecraft. Bottom-right drivers tend to be slower and lose positions on race day.")
        outcome_box("The best race outcomes come from combining speed with the ability to convert that speed into positions gained rather than positions defended or lost.")

        driver_perf = (
            df_racecraft.groupby(["FullName", "TeamName"], as_index=False)
            .agg(
                races=("round", "count"),
                avg_gain=("position_delta", "mean"),
                avg_pace=("relative_pace", "mean"),
            )
        )

        driver_perf = driver_perf[driver_perf["races"] >= MIN_RACES_DRIVER].copy()

        fig_quad = px.scatter(
            driver_perf,
            x="avg_pace",
            y="avg_gain",
            size="races",
            color="TeamName",
            hover_name="FullName",
            text="FullName",
            title=f"Which Drivers Combine Pace and Racecraft? ({season})",
        )

        fig_quad.add_hline(y=0, line_dash="dash")
        fig_quad.add_vline(x=0, line_dash="dash")

        fig_quad.update_xaxes(
            range=[driver_perf["avg_pace"].min() - 0.2, driver_perf["avg_pace"].max() + 0.2]
        )
        fig_quad.update_yaxes(
            range=[driver_perf["avg_gain"].min() - 0.5, driver_perf["avg_gain"].max() + 0.5]
        )

        fig_quad.update_layout(
            xaxis_title="Relative Pace (negative = faster than field)",
            yaxis_title="Average Positions Gained per Race",
            height=700,
        )

        fig_quad.update_traces(
            marker=dict(line=dict(width=1, color="black")),
            textposition="top center",
            textfont=dict(size=11),
        )
        st.plotly_chart(fig_quad, use_container_width=True)

    st.divider()
    methodology_box(
    "Methodology notes",
        """
        - Driver racecraft charts use **position_delta = GridPosition - FinishPosition** on classified drivers.
        - The pace/racecraft chart aggregates to the driver-season level using average relative pace and average positions gained.
        """,
        )


# =============================
# TRACKS TAB
# =============================
with tab_tracks:
    with st.container():
        section_header(
            "Which Tracks Produced the Most Overtaking?",
            "Estimated overtakes are derived from lap-to-lap position gains while excluding pit in/out laps, Lap 1 chaos, and non-green laps. The exact count is a proxy, but the ordering captures which circuits are more overtaking-friendly.",
        )
        insight_box("Tracks near the top tend to create more on-track movement. Monaco should sit near the bottom, while Las Vegas, Spa, or Monza typically rank much higher.")
        outcome_box("Some circuits make it far easier to recover positions after qualifying. Track design therefore changes how much race-day execution can alter finishing order.")

        if len(over_s) == 0:
            st.info(f"No overtakes data found for season {season}.")
        else:
            fig_ov = px.bar(
                over_s.sort_values("total_overtakes_est", ascending=True),
                x="total_overtakes_est",
                y="event_name",
                orientation="h",
                hover_data=["round", "drivers_count"],
                title=f"Which Tracks Produced the Most Overtaking? ({season})",
                labels={"total_overtakes_est": "Estimated Overtakes", "event_name": "Race"},
            )
            st.plotly_chart(fig_ov, use_container_width=True)

    st.divider()

    with st.container():
        section_header(
            "Where Does Qualifying Matter the Most?",
            "This chart measures how strongly starting position predicts finishing position at each track using a per-track Spearman correlation.",
        )
        insight_box("Higher correlation means grid order tends to persist through the race. Lower correlation means the circuit allows more reshuffling after qualifying.")
        outcome_box("Tracks with stronger grid-to-finish correlation reward qualifying more heavily, while lower-correlation tracks leave more room for overtakes, strategy, and recovery drives.")

        if len(df_grid) == 0:
            st.info(f"No valid grid-position data found for season {season}.")
        else:
            track_corr_rows = []
            for track_name, g in df_grid.groupby("event_name"):
                corr_val = safe_spearman(g, "GridPosition", "FinishPosition")
                if corr_val is not None:
                    track_corr_rows.append({
                        "event_name": track_name,
                        "spearman_grid_finish": corr_val,
                        "n": len(g),
                    })

            track_corr = pd.DataFrame(track_corr_rows).sort_values("spearman_grid_finish", ascending=True)

            fig_track_corr = px.bar(
                track_corr,
                x="spearman_grid_finish",
                y="event_name",
                orientation="h",
                hover_data=["n"],
                title=f"Where Does Qualifying Matter the Most? ({season})",
                labels={"spearman_grid_finish": "Spearman correlation (Grid vs Finish)", "event_name": "Track"},
            )
            st.plotly_chart(fig_track_corr, use_container_width=True)

    st.divider()
    methodology_box(
        "Methodology notes",
        """
        - Overtake counts are **estimated proxies** derived from lap-to-lap position gains, excluding pit in/out laps, Lap 1 chaos, and non-green laps.
        - The grid-advantage chart uses **Spearman correlation** between GridPosition and FinishPosition for each track in the selected season.
        - Track-level correlations are descriptive and can be noisy when unusual incidents materially shape a single race.
        """,
    )


# =============================
# TEAMS TAB
# =============================
with tab_teams:
    with st.container():
        section_header(
            "Team Race-Day Performance",
            "These charts compare how teams perform relative to their starting positions. Positive values indicate teams that tend to move forward on Sundays.",
        )
        insight_box("Backmarker teams often gain more positions because they start further back, while top teams have less room to improve. The boxplot helps separate consistent gains from occasional spikes.")
        outcome_box("Race outcomes are not determined by drivers alone. Team-level pace, pit strategy, and tire execution all affect whether a car finishes above or below its starting position.")

        team_perf = (
            df_racecraft.groupby("TeamName", as_index=False)
            .agg(
                races=("round", "nunique"),
                driver_race_rows=("position_delta", "size"),
                avg_positions_gained=("position_delta", "mean"),
                median_positions_gained=("position_delta", "median"),
                total_positions_gained=("position_delta", "sum"),
                total_points=("Points", "sum"),
            )
        )

        MIN_DRIVER_RACE_ROWS = 30
        team_perf = team_perf[team_perf["driver_race_rows"] >= MIN_DRIVER_RACE_ROWS].copy()
        team_perf = team_perf.sort_values("avg_positions_gained", ascending=False)

        col_team1, col_team2 = st.columns([1, 1])

        with col_team1:
            fig_team_bar = px.bar(
                team_perf,
                x="avg_positions_gained",
                y="TeamName",
                orientation="h",
                color="TeamName",
                hover_data=[
                    "races",
                    "driver_race_rows",
                    "median_positions_gained",
                    "total_positions_gained",
                    "total_points",
                ],
                title=f"Team Racecraft: Avg Positions Gained per Driver-Race ({season})",
                labels={"TeamName": "Team Name", "avg_positions_gained": "Avg Positions Gained"},
            )
            fig_team_bar.add_vline(x=0, line_dash="dash")
            fig_team_bar.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=False)
            st.plotly_chart(fig_team_bar, use_container_width=True)

        with col_team2:
            team_order = (
                df_racecraft.groupby("TeamName")["position_delta"]
                .median()
                .sort_values()
                .index
                .tolist()
            )

            fig_team_box = px.box(
                df_racecraft,
                x="position_delta",
                y="TeamName",
                points="outliers",
                category_orders={"TeamName": team_order},
                title=f"Distribution of Positions Gained by Team ({season})",
                hover_data=["FullName", "season", "round"],
                labels={"TeamName": "Team Name", "position_delta": "Position Delta"},
            )
            fig_team_box.add_vline(x=0, line_dash="dash")
            st.plotly_chart(fig_team_box, use_container_width=True)

    st.divider()

    with st.container():
        section_header(
            "Which Teams Combine Pace and Racecraft?",
            "This quadrant chart separates team-level average race pace from team-level average positions gained, helping distinguish raw speed from Sunday execution.",
        )
        insight_box("Top-left teams combine stronger pace with positive racecraft. Bottom-right teams tend to lack both underlying speed and race-day position improvement.")
        outcome_box("The most complete teams are not just fast in clean air — they also convert that pace into track-position gains through race execution, tire management, and strategy.")

        team_quad = (
            df_racecraft.groupby("TeamName", as_index=False)
            .agg(
                races=("round", "nunique"),
                driver_race_rows=("position_delta", "size"),
                avg_gain=("position_delta", "mean"),
                avg_pace=("relative_pace", "mean"),
                total_points=("Points", "sum"),
            )
        )

        team_quad = team_quad[team_quad["driver_race_rows"] >= MIN_DRIVER_RACE_ROWS].copy()

        fig_team_quad = px.scatter(
            team_quad,
            x="avg_pace",
            y="avg_gain",
            size="total_points",
            color="TeamName",
            text="TeamName",
            hover_data=["races", "driver_race_rows", "total_points"],
            title=f"Team Racecraft vs Pace ({season})",
        )

        fig_team_quad.add_hline(y=0, line_dash="dash")
        fig_team_quad.add_vline(x=0, line_dash="dash")

        if len(team_quad) > 0:
            fig_team_quad.update_xaxes(
                range=[team_quad["avg_pace"].min() - 0.1, team_quad["avg_pace"].max() + 0.1]
            )
            fig_team_quad.update_yaxes(
                range=[team_quad["avg_gain"].min() - 0.3, team_quad["avg_gain"].max() + 0.3]
            )

        fig_team_quad.update_layout(
            xaxis_title="Average Relative Pace (negative = faster than field)",
            yaxis_title="Average Positions Gained per Driver-Race",
            height=700,
        )

        fig_team_quad.update_traces(
            marker=dict(line=dict(width=1, color="black")),
            textposition="top center",
            textfont=dict(size=11),
        )
        st.plotly_chart(fig_team_quad, use_container_width=True)

    st.divider()
    methodology_box(
        "Methodology notes",
        """
        - Team charts aggregate driver-level **position_delta** and **relative_pace** to the team level.
        - Positive values indicate that a team tends to improve positions on race days; negative values indicate the opposite.
        - More negative relative pace indicates a faster team relative to the field average.
        - Backmarker teams often have more room to gain positions because they typically qualify further back.
        """,
    )
