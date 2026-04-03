from __future__ import annotations

from typing import Any, Dict

import pandas as pd



def estimate_overtakes_from_laps(laps: pd.DataFrame) -> Dict[str, int]:
    """
    Estimate overtakes from lap-level position data.

    Method:
    - Exclude Lap 1 (start-related position changes)
    - Exclude pit-in and pit-out laps
    - Exclude non-green laps when TrackStatus is available
    - Count lap-to-lap position improvements as overtakes

    Returns
    -------
    dict
        {"total_overtakes_est": <int>}
    """
    needed = ["Driver", "LapNumber", "Position", "PitInTime", "PitOutTime"]
    missing = [c for c in needed if c not in laps.columns]
    if missing:
        raise KeyError(
            f"Missing required lap columns: {missing}. "
            f"Available columns sample: {list(laps.columns)[:30]}"
        )

    cols = needed + (["TrackStatus"] if "TrackStatus" in laps.columns else [])
    df = laps[cols].copy()

    df["LapNumber"] = pd.to_numeric(df["LapNumber"], errors="coerce")
    df["Position"] = pd.to_numeric(df["Position"], errors="coerce")
    df = df.dropna(subset=["Driver", "LapNumber", "Position"])

    # Remove Lap 1 chaos
    df = df[df["LapNumber"] > 1]

    # Keep only green-flag laps where TrackStatus is available.
    if "TrackStatus" in df.columns:
        track_status = pd.to_numeric(df["TrackStatus"], errors="coerce")
        df = df[track_status == 1]

    df = df.sort_values(["Driver", "LapNumber"])
    df["pos_change"] = df.groupby("Driver")["Position"].diff()

    gained = df["pos_change"] < 0
    pit_lap = df["PitInTime"].notna() | df["PitOutTime"].notna()

    return {"total_overtakes_est": int((gained & ~pit_lap).sum())}



def overtake_breakdown(laps: pd.DataFrame, *, exclude_lap1: bool = True) -> Dict[str, Any]:
    """
    Diagnostic helper for understanding how estimated overtakes are distributed.

    Breaks down position gains into:
    - pit-related vs non-pit
    - green vs non-green laps when TrackStatus is available
    """
    base_cols = ["Driver", "LapNumber", "Position", "PitInTime", "PitOutTime"]
    extra_cols = ["TrackStatus"] if "TrackStatus" in laps.columns else []
    df = laps[base_cols + extra_cols].copy()

    df["LapNumber"] = pd.to_numeric(df["LapNumber"], errors="coerce")
    df["Position"] = pd.to_numeric(df["Position"], errors="coerce")
    df = df.dropna(subset=["Driver", "LapNumber", "Position"])

    if exclude_lap1:
        df = df[df["LapNumber"] > 1]

    df = df.sort_values(["Driver", "LapNumber"])
    df["pos_change"] = df.groupby("Driver")["Position"].diff()
    gained = df["pos_change"] < 0

    pit_lap = df["PitInTime"].notna() | df["PitOutTime"].notna()

    has_ts = "TrackStatus" in df.columns
    if has_ts:
        ts = pd.to_numeric(df["TrackStatus"], errors="coerce")
        green_lap = ts.eq(1)
        non_green_lap = ~green_lap & ts.notna()
        ts_missing = ts.isna()
    else:
        green_lap = pd.Series(False, index=df.index)
        non_green_lap = pd.Series(False, index=df.index)
        ts_missing = pd.Series(True, index=df.index)

    total = int(gained.sum())
    pit_related = int((gained & pit_lap).sum())
    non_pit = int((gained & ~pit_lap).sum())

    result: Dict[str, Any] = {
        "total_position_gains": total,
        "gains_on_pit_in_or_out_laps": pit_related,
        "gains_on_non_pit_laps": non_pit,
        "share_non_pit": (non_pit / total) if total else None,
    }

    if has_ts:
        green_gains = int((gained & green_lap).sum())
        non_green_gains = int((gained & non_green_lap).sum())
        unknown_ts_gains = int((gained & ts_missing).sum())
        result.update(
            {
                "gains_on_green_laps_only": green_gains,
                "gains_on_non_green_laps": non_green_gains,
                "gains_on_unknown_trackstatus_laps": unknown_ts_gains,
                "share_green": (green_gains / total) if total else None,
                "share_non_green": (non_green_gains / total) if total else None,
                "gains_green_non_pit": int((gained & green_lap & ~pit_lap).sum()),
                "gains_non_green_non_pit": int((gained & non_green_lap & ~pit_lap).sum()),
                "gains_green_pit": int((gained & green_lap & pit_lap).sum()),
                "gains_non_green_pit": int((gained & non_green_lap & pit_lap).sum()),
            }
        )
    else:
        result["note"] = "TrackStatus not available in laps dataframe."

    return result
