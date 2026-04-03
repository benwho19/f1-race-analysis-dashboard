from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

import fastf1
import pandas as pd

from estimators import estimate_overtakes_from_laps



def build_overtakes_by_race_resumable(
    seasons: List[int],
    *,
    rounds: Optional[List[int]] = None,
    session_code: str = "R",
    cache_dir: str = "./data/cache",
    save_path: str = "./data/processed/overtakes_by_race.parquet",
    stop_on_rate_limit: bool = True,
    per_race_sleep_s: float = 0.25,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resumable builder for race-level overtake estimates.

    Produces one row per race with an estimated overtake count and race metadata.
    """
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    if os.path.exists(save_path):
        df_existing = pd.read_parquet(save_path)
        df_existing["season"] = pd.to_numeric(df_existing["season"], errors="coerce").astype("Int64")
        df_existing["round"] = pd.to_numeric(df_existing["round"], errors="coerce").astype("Int64")
        completed = set(
            (int(s), int(r), str(sc))
            for s, r, sc in zip(df_existing["season"], df_existing["round"], df_existing["session_code"])
            if pd.notna(s) and pd.notna(r)
        )
        parts = [df_existing]
        print(f"Loaded existing overtakes dataset: {save_path} (rows={len(df_existing):,})")
    else:
        completed = set()
        parts = []
        print("No existing overtakes dataset found. Starting fresh.")

    error_rows: List[Dict[str, Any]] = []

    def is_rate_limit_error(exc: Exception) -> bool:
        msg = repr(exc)
        return ("RateLimitExceededError" in msg) or ("500 calls/h" in msg)

    rounds_set = set(rounds) if rounds is not None else None

    for season in seasons:
        print(f"\n=== Overtakes | Season {season} ===")

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
            key = (season, round_no, session_code)

            if key in completed:
                continue

            print(f"  -> {season} Round {round_no}: {gp} ({session_code})")

            try:
                session = fastf1.get_session(season, gp, session_code)
                session.load(laps=True, telemetry=False, weather=False, messages=False)

                laps = session.laps
                if laps.empty:
                    raise ValueError("session.laps is empty")

                est = estimate_overtakes_from_laps(laps)
                total_overtakes_est = int(est["total_overtakes_est"])

                driver_col = "Driver" if "Driver" in laps.columns else ("DriverNumber" if "DriverNumber" in laps.columns else None)
                if driver_col is None:
                    raise KeyError(f"No driver column found in laps. Available columns sample: {list(laps.columns)[:30]}")
                drivers_count = int(laps[driver_col].nunique())

                event = session.event
                out_row = {
                    "season": season,
                    "round": round_no,
                    "event_name": getattr(event, "EventName", gp),
                    "location": getattr(event, "Location", pd.NA),
                    "country": getattr(event, "Country", pd.NA),
                    "session_code": session_code,
                    "drivers_count": drivers_count,
                    "total_overtakes_est": total_overtakes_est,
                }

                parts.append(pd.DataFrame([out_row]))
                completed.add(key)

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
        print(f"\nSaved overtakes dataset: {save_path} (rows={len(df_all):,})")
    else:
        print("\nNo overtakes data produced; nothing saved.")

    if not errors.empty:
        err_path = save_path.replace(".parquet", "_errors.parquet")
        errors.to_parquet(err_path, index=False)
        print(f"Saved errors: {err_path} (rows={len(errors):,})")

    return df_all, errors


if __name__ == "__main__":
    build_overtakes_by_race_resumable(seasons=[2020, 2021, 2022, 2023, 2024, 2025])
