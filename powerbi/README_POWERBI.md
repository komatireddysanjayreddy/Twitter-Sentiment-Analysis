# Connecting Power BI to the Twitter Sentiment PostgreSQL Database

## Prerequisites

1. Power BI Desktop (free from Microsoft Store)
2. PostgreSQL ODBC Driver — install from [odbc.postgresql.org](https://odbc.postgresql.org/docs/unix-compilation.html)
   OR use the native connector (below — no ODBC needed for PostgreSQL 10+)
3. Your PostgreSQL must be reachable from the machine running Power BI:
   - **Local**: Docker port `5432` is exposed to `localhost`
   - **Cloud**: Use a managed PostgreSQL (Neon, Supabase, Railway, AWS RDS)
     and whitelist your IP in the firewall rules

---

## Step 1 — Get Data → PostgreSQL

1. Open Power BI Desktop
2. Click **Home → Get Data → More…**
3. Search for **PostgreSQL** → select it → click **Connect**
4. Fill in the connection dialog:

| Field | Value |
|---|---|
| Server | `localhost` (local) or your cloud host |
| Database | `sentiment_db` (or value of `POSTGRES_DB`) |
| Data Connectivity mode | **DirectQuery** (live data) or **Import** (snapshot) |

5. Click **OK** → enter credentials:
   - Username: value of `POSTGRES_USER`
   - Password: value of `POSTGRES_PASSWORD`

> **Tip**: Use **DirectQuery** for real-time dashboards so Power BI queries
> the database fresh on each page refresh.

---

## Step 2 — Select Tables / Views

After connecting, select these objects in the Navigator:

| Object | Type | Purpose |
|---|---|---|
| `tweet_sentiments` | Table | Raw data for drill-through |
| `v_sentiment_summary` | View | KPI card tiles |
| `mv_sentiment_per_minute` | Mat. View | Time-series chart |
| `mv_top_hashtags` | Mat. View | Hashtag bar chart |

---

## Step 3 — Build the Report

### Page 1: Overview

**KPI Cards** (from `v_sentiment_summary`):
- Total Tweets → `total_tweets`
- Positive % → `positive_pct`
- Avg Sentiment Score → `avg_compound_score`

**Donut Chart** (from `v_sentiment_summary`):
- Values: `positive_count`, `negative_count`, `neutral_count`

### Page 2: Time-Series

**Line Chart** (from `mv_sentiment_per_minute`):
- X-axis: `bucket`
- Y-axis: `tweet_count`
- Legend: `sentiment`

### Page 3: Hashtag Analysis

**Bar Chart** (from `mv_top_hashtags`):
- Axis: `hashtag`
- Value: `mention_count`
- Color: `avg_score` (conditional formatting: green = positive, red = negative)

### Page 4: Tweet Explorer

**Table** (from `tweet_sentiments`):
- Columns: `author`, `text`, `sentiment`, `compound_score`, `processed_at`
- Add a **Slicer** on `sentiment` for filtering

---

## Step 4 — Auto-Refresh (DirectQuery)

In DirectQuery mode, each visual queries live data.

To add a timed page refresh:
1. Select the report page
2. **View → Performance analyzer** → verify queries are fast (<2s)
3. **File → Options → Current File → Auto page refresh**
   - Minimum interval: **10 seconds** (matches Spark micro-batch trigger)

---

## Step 5 — Publish to Power BI Service (optional)

1. **File → Publish → Publish to Power BI**
2. Choose your workspace
3. In Power BI Service, configure the **Gateway** to allow access to your PostgreSQL
4. Schedule dataset refresh or keep as DirectQuery

---

## Useful DAX Measures

```dax
Positive Rate =
DIVIDE(
    CALCULATE(COUNT(tweet_sentiments[sentiment]), tweet_sentiments[sentiment] = "positive"),
    COUNT(tweet_sentiments[sentiment])
) * 100

Rolling 1hr Avg Sentiment =
CALCULATE(
    AVERAGE(tweet_sentiments[compound_score]),
    FILTER(
        tweet_sentiments,
        tweet_sentiments[processed_at] >= NOW() - (1/24)
    )
)
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| "Unable to connect to server" | Check Docker is running, port 5432 is exposed |
| "SSL required" | In connection settings, set SSL mode to `Disable` for local dev |
| Materialized views empty | Run `SELECT refresh_materialized_views();` in psql |
| Data stale | Refresh materialized views; check Spark processor is running |
