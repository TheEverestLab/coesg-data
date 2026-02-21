# coesg-data

Pre-processed COE (Certificate of Entitlement) bidding data for Singapore, served via GitHub Pages CDN.

## CDN Endpoints

| Endpoint | Description |
|----------|-------------|
| [`v1/latest.json`](https://theeverestlab.github.io/coesg-data/v1/latest.json) | Latest + previous round (`COELatestSnapshot`) |
| [`v1/history.json`](https://theeverestlab.github.io/coesg-data/v1/history.json) | All rounds (`[COERoundResult]`) |

## How It Works

1. **Fetch** — GitHub Actions cron runs every 6h, calling `scripts/fetch_coe_data.py`
2. **Transform** — Python script fetches from [data.gov.sg](https://data.gov.sg) API, groups flat records into round-based JSON
3. **Commit** — Only commits if data actually changed (diff check)
4. **Deploy** — Push to `main` triggers GitHub Pages deployment automatically

## Data Source

Official Singapore government data from [data.gov.sg](https://data.gov.sg/datasets/d_69b3380ad7e51aff3a7dcc84eba52b8a/view) — COE Bidding Results dataset.

## JSON Format

Dates use ISO 8601 UTC (Z suffix). Category keys are single uppercase letters: A, B, C, D, E.

```json
// latest.json
{
  "latestRound": { "id": "2026-02-1", "biddingDate": "...", "roundLabel": "Feb 2026 Ex 1", "prices": {"A": 92000, ...}, ... },
  "previousRound": { ... },
  "lastUpdated": "2026-02-19T06:00:00Z"
}

// history.json — array of COERoundResult sorted by date descending
[{ "id": "2026-02-1", ... }, { "id": "2026-01-2", ... }, ...]
```

## Used By

- [COE SG](https://github.com/TheEverestLab/coesg) — iOS app + widget
