from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import fastf1
import pandas as pd


def process_race(season: int, gp: str, session_code: str = "R") -> pd.DataFrame:
    """
    Build one driver-race dataframe for a single event.

    The goal is to produce one row per classified driver with race-level metrics
    that can power the dashboard.
    """
    session = fastf1.get_session(season, gp, session_code)
    session.load(laps=True, telemetry=False, weather=False, messages=False)

    laps = session.laps.copy()
    results = session.results.copy()

    if laps.empty or results.empty:
        return pd.DataFrame()

    # Keep only usable laps for pace/consistency metrics.
    clean_laps = laps.copy()
    if "LapTime" in clean_laps.columns:
        clean_laps = clean_laps[clean_laps["LapTime"].notna()].copy()
    if "PitOutTime" in clean_laps.columns:
        clean_laps = clean_laps[clean_laps["PitOutTime"].isna()].copy()
    if "PitInTime" in clean_laps.columns:
        clean_laps = clean_laps[clean_laps["PitInTime"].isna()].copy()
    if "TrackStatus" in clean_laps.columns:
        ts = pd.to_numeric(clean_laps["TrackStatus"], errors="coerce")
        clean_laps = clean_laps[ts == 1].copy()

    # Convert lap time to seconds for aggregated metrics.
    if "LapTime" in clean_laps.columns:
        clean_laps["LapTimeSeconds"] = clean_laps["LapTime"].dt.total_seconds()
    else:
        clean_laps["LapTimeSeconds"] = pd.NA

    field_avg_lap = clean_laps["LapTimeSeconds"].mean()

    per_driver_lap = (
        clean_laps.groupby("Driver", as_index=False)
        .agg(
            avg_lap_time=("LapTimeSeconds", "mean"),
            lap_time_std=("LapTimeSeconds", "std"),
            clean_lap_count=("LapTimeSeconds", "count"),
            stint_count=("Stint", "nunique") if "Stint" in clean_laps.columns else ("Driver", "size"),
        )
    )

    per_driver_lap["relative_pace"] = per_driver_lap["avg_lap_time"] - field_avg_lap
    per_driver_lap["consistency_index"] = 1 / per_driver_lap["lap_time_std"].replace(0, pd.NA)

    results_subset = results.copy()
    rename_map = {
        "Abbreviation": "Driver",
        "Position": "FinishPosition",
        "GridPosition": "GridPosition",
        "Points": "Points",
        "FullName": "FullName",
        "TeamName": "TeamName",
        "Status": "Status",
        "DriverNumber": "DriverNumber",
    }
    results_subset = results_subset.rename(columns=rename_map)

    keep_cols = [
        "Driver",
        "DriverNumber",
        "FullName",
        "TeamName",
        "GridPosition",
        "FinishPosition",
        "Points",
        "Status",
    ]
    results_subset = results_subset[[c for c in keep_cols if c in results_subset.columns]].copy()

    for col in ["GridPosition", "FinishPosition", "Points"]:
        if col in results_subset.columns:
            results_subset[col] = pd.to_numeric(results_subset[col], errors="coerce")

    out = results_subset.merge(per_driver_lap, on="Driver", how="left")

    out["position_delta"] = out["GridPosition"] - out["FinishPosition"]
    out["normalized_delta"] = out["position_delta"] / out["GridPosition"].replace(0, pd.NA)
    out["had_disruption"] = out["Status"].astype(str).str.contains(
        "Accident|Collision|Engine|Gearbox|DNF|Retired|Disqualified",
        case=False,
        na=False,
    ).astype(int)

    event = session.event
    out["season"] = season
    out["round"] = int(getattr(event, "RoundNumber", pd.NA))
    out["event_name"] = getattr(event, "EventName", gp)
    out["location"] = getattr(event, "Location", pd.NA)
    out["country"] = getattr(event, "Country", pd.NA)
    out["session_code"] = session_code

    desired_order = [
        "season",
        "round",
        "event_name",
        "location",
        "country",
        "session_code",
        "Driver",
        "DriverNumber",
        "FullName",
        "TeamName",
        "GridPosition",
        "FinishPosition",
        "Points",
        "Status",
        "position_delta",
        "normalized_delta",
        "avg_lap_time",
        "relative_pace",
        "lap_time_std",
        "consistency_index",
        "clean_lap_count",
        "stint_count",
        "had_disruption",
    ]

    return out[[c for c in desired_order if c in out.columns]].copy()



def build_driver_race_dataset_resumable(
    seasons: List[int],
    *,
    rounds: Optional[List[int]] = None,
    session_code: str = "R",
    cache_dir: str = "./data/cache",
    save_path: str = "./data/processed/driver_race_metrics.parquet",
    stop_on_rate_limit: bool = True,
    per_race_sleep_s: float = 0.5,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resumable dataset builder for driver race metrics.

    Notes
    -----
    - Loads an existing parquet if present and skips completed races.
    - Uses FastF1 cache to avoid repeated downloads.
    - Safe to rerun after interruptions.
    """
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    if os.path.exists(save_path):
        df_existing = pd.read_parquet(save_path)
        df_existing["season"] = pd.to_numeric(df_existing["season"], errors="coerce").astype("Int64")
        df_existing["round"] = pd.to_numeric(df_existing["round"], errors="coerce").astype("Int64")
        completed = set(
            (int(s), int(r))
            for s, r in zip(df_existing["season"], df_existing["round"])
            if pd.notna(s) and pd.notna(r)
        )
        parts = [df_existing]
        print(f"Loaded existing dataset: {save_path} (rows={len(df_existing):,})")
    else:
        completed = set()
        parts = []
        print("No existing dataset found. Starting fresh.")

    error_rows: List[Dict[str, Any]] = []

    def is_rate_limit_error(exc: Exception) -> bool:
        msg = repr(exc)
        return ("RateLimitExceededError" in msg) or ("500 calls/h" in msg)

    rounds_set = set(rounds) if rounds is not None else None

    for season in seasons:
        print(f"\n=== Season {season} ===")

        try:
            schedule = fastf1.get_event_schedule(season)
        except Exception as exc:
            error_rows.append({"season": season, "round": None, "event_name": None, "error": repr(exc)})
            if is_rate_limit_error(exc) and stop_on_rate_limit:
                print("Hit rate limit while fetching schedule. Stopping early.")
                break
            continue

        schedule = schedule[~schedule["EventName"].astype(str).str.contains("Test", case=False, na=False)]
        schedule = schedule[pd.to_numeric(schedule["RoundNumber"], errors="coerce") > 0]
        if rounds_set is not None:
            schedule = schedule[pd.to_numeric(schedule["RoundNumber"], errors="coerce").isin(rounds_set)]
        schedule = schedule.sort_values("RoundNumber")

        for _, row in schedule.iterrows():
            gp = str(row["EventName"])
            round_no = int(row["RoundNumber"])

            if (season, round_no) in completed:
                continue

            print(f"  -> {season} Round {round_no}: {gp} ({session_code})")

            try:
                df_race = process_race(season, gp, session_code)
                if df_race.empty:
                    raise ValueError("process_race returned empty dataframe")

                parts.append(df_race)
                completed.add((season, round_no))

                df_all = pd.concat(parts, ignore_index=True)
                df_all.to_parquet(save_path, index=False)
                print(f"     Saved progress (rows={len(df_all):,})")

            except Exception as exc:
                error_rows.append(
                    {
                        "season": season,
                        "round": round_no,
                        "event_name": gp,
                        "session_code": session_code,
                        "error": repr(exc),
                    }
                )
                print(f"     !! Failed: {repr(exc)}")

                if is_rate_limit_error(exc) and stop_on_rate_limit:
                    print("Hit rate limit. Saving progress and stopping early.")
                    df_all = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
                    if not df_all.empty:
                        df_all.to_parquet(save_path, index=False)
                    errors = pd.DataFrame(error_rows)
                    err_path = save_path.replace(".parquet", "_errors.parquet")
                    errors.to_parquet(err_path, index=False)
                    return df_all, errors

            if per_race_sleep_s and per_race_sleep_s > 0:
                time.sleep(per_race_sleep_s)

    df_all = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    errors = pd.DataFrame(error_rows)

    if not df_all.empty:
        df_all.to_parquet(save_path, index=False)
        print(f"\nSaved dataset: {save_path} (rows={len(df_all):,})")
    else:
        print("\nNo data produced; nothing saved.")

    if not errors.empty:
        err_path = save_path.replace(".parquet", "_errors.parquet")
        errors.to_parquet(err_path, index=False)
        print(f"Saved errors: {err_path} (rows={len(errors):,})")

    return df_all, errors


if __name__ == "__main__":
    build_driver_race_dataset_resumable(seasons=[2020, 2021, 2022, 2023, 2024, 2025])
