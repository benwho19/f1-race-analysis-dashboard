# Dataset Instructions

This project includes pre-generated datasets for convenience.
You can run the dashboard directly using these files without rebuilding the data.

However, instructions are also provided below to **recreate the dataset from scratch** using the data pipeline.

---

## Data Source

All data is sourced from the Python library FastF1, which provides access to official Formula 1 timing data.

The dataset covers the **2020–2025 seasons**.

---

## Included Datasets

The following processed datasets are included in this repository:

```text
data/processed/
├── driver_race_metrics.parquet
├── overtakes_by_race.parquet
```

These files are sufficient to run the Streamlit dashboard.

---

## Running the Dashboard

No data setup is required.

Simply run:

```bash
streamlit run app/streamlit_app.py
```

---

## Rebuilding the Dataset (Optional)

If you would like to regenerate the dataset:

### 1. Install dependencies

```bash
pip install fastf1 pandas pyarrow
```

---

### 2. Enable FastF1 cache

```python
import fastf1
fastf1.Cache.enable_cache("./data/cache")
```

---

### 3. Run dataset builders


#### Included Scripts

The following files are included in this repository:

```text
data/data_pipeline/
├── build_driver_race_dataset.py
├── build_overtakes_dataset.py
├── estimators.py
```

#### Driver race dataset

```python
from data_pipeline.build_driver_race_dataset import build_driver_race_dataset_resumable

build_driver_race_dataset_resumable(
    seasons=[2020, 2021, 2022, 2023, 2024, 2025]
)
```

---

#### Overtakes dataset

```python
from data_pipeline.build_overtakes_dataset import build_overtakes_by_race_resumable

build_overtakes_by_race_resumable(
    seasons=[2020, 2021, 2022, 2023, 2024, 2025]
)
```

---

#### Runtime notes

Runtime notes:
* Initial run may take **30–60+ minutes** depending on cache state
* FastF1 rate limits (~500 requests/hour) may pause execution
* If execution is paused due to rate limits, the pipeline is resumable and will continue where it left off


More info:
* build_overtakes_dataset.py imports estimate_overtakes_from_laps from estimators.py
* the driver dataset builder includes a process_race() function and a resumable build_driver_race_dataset_resumable() function
* the overtakes builder includes a resumable build_overtakes_by_race_resumable() function

One important caveat:
* these files are a clean, reusable first pass, but you may need to adjust path assumptions and possibly column names if your exact processed dataset schema differs slightly from what we standardized in the dashboard

---

## Overtake Estimation Methodology

Overtakes are not directly provided by the API and are estimated using lap-level data.

The estimator:

* Tracks position changes between consecutive laps
* Counts position gains as overtakes
* Excludes:

  * Lap 1 (start-related movement)
  * Pit-in and pit-out laps
  * Non-green flag laps (e.g. safety cars)

> Note: These are **estimated overtakes**, not official FIA values.
> The dataset is intended for **relative comparisons across tracks**, not exact counts.

---

## Reproducibility

The included datasets represent a **frozen version (last updated March 2026)** of the data used in the dashboard.

If you regenerate the dataset:

* Results may differ slightly due to:

  * API updates
  * race data corrections
  * estimation methodology changes

---

## Limitations

* Overtake counts are estimates
* Race-specific events (weather, safety cars) introduce variability
* Some metrics (e.g., fastest lap, pit stop time) are approximated or excluded

---

## Summary

* You can run the dashboard immediately using the included datasets
* The pipeline is provided for transparency and reproducibility
* Results are designed for **comparative analysis**, not exact official statistics

---
