# 🏎️ F1 Race Outcome Analysis Dashboard

An interactive Streamlit dashboard analyzing what drives race outcomes in Formula 1.
This project explores how qualifying position, race pace, and in-race performance relate to finishing position, and allows users to compare drivers, teams, and tracks across seasons.

---

## Live App

https://formula1-analysis-dashboard.streamlit.app/

---

## Overview

What determines success in a Formula 1 race?

This project analyzes race results, qualifying data, and performance metrics to identify key factors associated with better race outcomes. The analysis is presented through an interactive dashboard that allows users to explore:

* Drivers who outperform expectations
* Teams that combine pace and racecraft
* Tracks with unique race characteristics
* Key variables associated with race performance

---

## Key Insights

* **Starting position (grid position)** is one of the strongest predictors of race outcome
* **Positions gained during the race** is highly associated with better finishes
* **Faster relative pace** strongly correlates with improved race performance
* **Fastest lap** is a strong performance signal

---

## Dashboard Features

### Overview

* Key performance factors correlated with race outcomes
* Relationship between qualifying position and finishing position
* Team-level comparisons of qualifying vs. race performance

### Drivers

* Drivers who consistently gain positions on race day
* Distribution of positions gained by driver
* Identification of drivers combining **pace vs. racecraft**

### Teams

* Team-level racecraft (positions gained)
* Distribution of race-day performance
* Team quadrant: **pace vs. racecraft**

### Tracks

* Track-level variation in overtaking and importance of qualifying position

---

## Methodology

### Feature Engineering

* **Positions gained** = Finish Position − Grid Position
* **Relative pace** = proxy for race speed (lower = faster)
* **Fastest lap indicator** = whether a driver set the fastest lap in a race

### Correlation Analysis

* Used **Spearman correlation** to measure monotonic relationships
* Transformed variables so that **higher values consistently represent better performance**

  * Improves interpretability of results
  * Ensures consistent direction across features

### Important Note

* Correlation does **not imply causation**
* Some variables (e.g., fastest lap) are **performance signals**, not predictive inputs

---

## Tech Stack

* **Python**
* **Pandas** (data manipulation, Spearman correlation)
* **NumPy** (numerical operations)
* **Plotly** (interactive visualizations)
* **Streamlit** (dashboard framework)
* **FastF1** (data ingestion, lap data)
* **PyArrow** (parquet file read/write)

---

## Project Structure

```
f1-race-outcome-dashboard/
│
├── streamlit_app.py
├── requirements.txt
├── README.md
├── .gitignore
│
├── data/
│   └── dataset_instructions.md
│   └── processed
│       └── parquet files
│   └── data_pipeline/
│       └── pipeline script files
│
└── assets/
    └── screenshots

```

---

## How to Run Locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

---

## Future Improvements

* Add predictive modeling (e.g., finish position prediction)
* Incorporate pit stop strategy and tire data
* Add driver/team filtering for deeper comparisons

---

## Why This Project

This project demonstrates:

* Translating exploratory analysis into an **interactive product**
* Designing intuitive visualizations for non-technical users
* Improving interpretability through **feature transformation**
* Communicating insights clearly through a dashboard interface
