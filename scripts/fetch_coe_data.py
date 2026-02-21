#!/usr/bin/env python3
"""
Fetch COE bidding results from data.gov.sg and write pre-processed JSON files.

Output files (matching Swift Codable format):
  v1/latest.json  — COELatestSnapshot
  v1/history.json — [COERoundResult]

Run manually:   python3 scripts/fetch_coe_data.py
Run via CI:     .github/workflows/fetch.yml
"""

import json
import ssl
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from calendar import monthrange

# ── Config ──────────────────────────────────────────────────────────────

DATA_GOV_URL = "https://data.gov.sg/api/action/datastore_search"
RESOURCE_ID = "d_69b3380ad7e51aff3a7dcc84eba52b8a"
FETCH_LIMIT = 1000  # ~200 rounds × 5 categories — 4+ years of history

SGT = timezone(timedelta(hours=8))

# Output directory (relative to repo root)
REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "v1"

# Maps data.gov.sg "vehicle_class" to our short category key
CATEGORY_MAP = {
    "Category A": "A",
    "Category B": "B",
    "Category C": "C",
    "Category D": "D",
    "Category E": "E",
}


# ── Helpers ─────────────────────────────────────────────────────────────

def parse_int(s: str) -> int:
    """Parse an integer from a string that may contain commas."""
    return int(s.replace(",", ""))


def nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> int:
    """Return the day-of-month for the nth occurrence of weekday (0=Mon..6=Sun)."""
    first_day_weekday = datetime(year, month, 1).weekday()  # 0=Mon
    # Days until first occurrence of target weekday
    diff = (weekday - first_day_weekday) % 7
    day = 1 + diff + (n - 1) * 7
    # Clamp to valid range for the month
    _, max_day = monthrange(year, month)
    return min(day, max_day)


def bidding_date_for(month_str: str, bidding_no: int) -> str:
    """
    Compute the bidding close datetime for a round.
    Bidding closes at 16:00 SGT (08:00 UTC) on the 1st or 3rd Wednesday.
    Returns ISO 8601 string in UTC (Z suffix) for maximum Swift compatibility.
    """
    year, month = int(month_str[:4]), int(month_str[5:7])
    ordinal = 1 if bidding_no == 1 else 3
    wed_day = nth_weekday_of_month(year, month, 2, ordinal)  # 2 = Wednesday (0=Mon)
    dt_sgt = datetime(year, month, wed_day, 16, 0, 0, tzinfo=SGT)
    dt_utc = dt_sgt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def round_label_for(month_str: str, bidding_no: int) -> str:
    """E.g. 'Jan 2025 Ex 1'."""
    dt = datetime.strptime(month_str, "%Y-%m")
    return f"{dt.strftime('%b %Y')} Ex {bidding_no}"


# ── Main ────────────────────────────────────────────────────────────────

MAX_RETRIES = 4
RETRY_DELAYS = [10, 20, 40, 60]  # seconds


def _make_ssl_context() -> ssl.SSLContext:
    """Create an SSL context, trying certifi first, then system certs."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        pass
    # Fallback: default context (works on CI / most Linux)
    return ssl.create_default_context()


def fetch_records() -> list[dict]:
    """Fetch raw records from data.gov.sg with retry on rate limiting."""
    params = (
        f"resource_id={RESOURCE_ID}"
        f"&limit={FETCH_LIMIT}"
        f"&sort=month+desc,+bidding_no+desc"
    )
    url = f"{DATA_GOV_URL}?{params}"

    ctx = _make_ssl_context()

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"Fetching {url} (attempt {attempt}/{MAX_RETRIES})")

        req = urllib.request.Request(url)
        req.add_header("User-Agent", "COE-SG-GitHub-Actions/1.0")

        try:
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt - 1]
                print(f"Rate limited (429), retrying in {delay}s...")
                time.sleep(delay)
                continue
            print(f"HTTP error {e.code}: {e.reason}", file=sys.stderr)
            sys.exit(1)
        except urllib.error.URLError as e:
            print(f"URL error: {e.reason}", file=sys.stderr)
            sys.exit(1)

        # Handle rate limit returned as JSON (non-HTTP-429 variant)
        if data.get("code") == 24 or data.get("name") == "TOO_MANY_REQUESTS":
            if attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt - 1]
                print(f"Rate limited (JSON), retrying in {delay}s...")
                time.sleep(delay)
                continue
            print("Rate limit exceeded after all retries", file=sys.stderr)
            sys.exit(1)

        if not data.get("success"):
            print(f"API returned success=false: {json.dumps(data)[:200]}", file=sys.stderr)
            sys.exit(1)

        records = data["result"]["records"]
        print(f"Fetched {len(records)} records")
        return records

    print("All retry attempts exhausted", file=sys.stderr)
    sys.exit(1)


def group_into_rounds(records: list[dict]) -> list[dict]:
    """
    Group flat API records into COERoundResult-shaped dicts.
    Returns sorted by biddingDate descending.
    """
    grouped: dict[str, dict] = {}

    for rec in records:
        month = rec["month"]
        bidding_no = int(rec["bidding_no"])
        key = f"{month}-{bidding_no}"

        if key not in grouped:
            grouped[key] = {
                "id": key,
                "biddingDate": bidding_date_for(month, bidding_no),
                "roundLabel": round_label_for(month, bidding_no),
                "prices": {},
                "quotas": {},
                "bidsReceived": {},
                "bidsSuccess": {},
            }

        cat_key = rec.get("vehicle_class", "")
        cat = CATEGORY_MAP.get(cat_key)
        if not cat:
            continue

        grouped[key]["prices"][cat] = parse_int(rec.get("premium", "0"))
        grouped[key]["quotas"][cat] = parse_int(rec.get("quota", "0"))
        grouped[key]["bidsReceived"][cat] = parse_int(rec.get("bids_received", "0"))
        grouped[key]["bidsSuccess"][cat] = parse_int(rec.get("bids_success", "0"))

    # Sort by biddingDate descending
    rounds = sorted(grouped.values(), key=lambda r: r["biddingDate"], reverse=True)
    return rounds


def build_latest_snapshot(rounds: list[dict]) -> dict:
    """Build a COELatestSnapshot-shaped dict from the first two rounds."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "latestRound": rounds[0] if len(rounds) > 0 else None,
        "previousRound": rounds[1] if len(rounds) > 1 else None,
        "lastUpdated": now,
    }


def write_json(path: Path, data) -> None:
    """Write JSON to file with consistent formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"Wrote {path} ({path.stat().st_size} bytes)")


def build_analytics(rounds: list[dict]) -> dict:
    """
    Build per-category analytics from historical rounds.
    Output: v1/analytics.json
    """
    import statistics

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    categories = ["A", "B", "C", "D", "E"]
    per_category = {}

    for cat in categories:
        prices_with_dates = []
        for r in rounds:
            p = r["prices"].get(cat)
            if p is not None:
                prices_with_dates.append((p, r["biddingDate"]))

        if not prices_with_dates:
            continue

        prices = [pw[0] for pw in prices_with_dates]
        avg = int(statistics.mean(prices))
        med = int(statistics.median(prices))

        min_entry = min(prices_with_dates, key=lambda x: x[0])
        max_entry = max(prices_with_dates, key=lambda x: x[0])

        # YoY change: latest price vs price ~1 year ago
        yoy_change = None
        if len(prices_with_dates) >= 2:
            latest_price = prices_with_dates[0][0]  # rounds are sorted desc
            # Find the round closest to 1 year ago
            latest_date = datetime.fromisoformat(prices_with_dates[0][1].replace("Z", "+00:00"))
            one_year_ago = latest_date - timedelta(days=365)
            closest = min(
                prices_with_dates[1:],
                key=lambda x: abs(
                    (datetime.fromisoformat(x[1].replace("Z", "+00:00")) - one_year_ago).total_seconds()
                ),
            )
            yoy_change = latest_price - closest[0]

        per_category[cat] = {
            "average": avg,
            "median": med,
            "min": {"price": min_entry[0], "date": min_entry[1]},
            "max": {"price": max_entry[0], "date": max_entry[1]},
            "yoyChange": yoy_change,
            "dataPoints": len(prices),
        }

    return {
        "categories": per_category,
        "totalRounds": len(rounds),
        "generatedAt": now,
    }


def build_schedule() -> dict:
    """
    Build a schedule of the next 4 upcoming bidding dates.
    Output: v1/schedule.json
    """
    now_sgt = datetime.now(SGT)
    schedules = []
    year, month = now_sgt.year, now_sgt.month
    exercise = 1  # Start checking from exercise 1 of current month

    while len(schedules) < 4:
        for ex in (1, 3):
            if exercise > 1 and ex < exercise:
                continue
            wed_day = nth_weekday_of_month(year, month, 2, ex)  # 2 = Wednesday
            closing_sgt = datetime(year, month, wed_day, 16, 0, 0, tzinfo=SGT)

            if closing_sgt <= now_sgt:
                continue

            closing_utc = closing_sgt.astimezone(timezone.utc)
            bidding_no = 1 if ex == 1 else 2
            month_str = f"{year}-{month:02d}"

            schedules.append({
                "closingDate": closing_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "roundLabel": round_label_for(month_str, bidding_no),
                "exerciseNumber": bidding_no,
            })

            if len(schedules) >= 4:
                break

        exercise = 1  # Reset for next month
        # Advance to next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "upcoming": schedules,
        "generatedAt": now,
    }


def main():
    records = fetch_records()

    if not records:
        print("No records returned, skipping write", file=sys.stderr)
        sys.exit(1)

    rounds = group_into_rounds(records)
    print(f"Grouped into {len(rounds)} rounds")

    if not rounds:
        print("No rounds after grouping, skipping write", file=sys.stderr)
        sys.exit(1)

    snapshot = build_latest_snapshot(rounds)

    write_json(OUTPUT_DIR / "latest.json", snapshot)
    write_json(OUTPUT_DIR / "history.json", rounds)

    # Pre-computed analytics
    analytics = build_analytics(rounds)
    write_json(OUTPUT_DIR / "analytics.json", analytics)

    # Bidding schedule
    schedule = build_schedule()
    write_json(OUTPUT_DIR / "schedule.json", schedule)

    # Summary
    latest = rounds[0]
    print(f"\nLatest round: {latest['roundLabel']} ({latest['biddingDate']})")
    for cat in ["A", "B", "C", "D", "E"]:
        price = latest["prices"].get(cat, "N/A")
        print(f"  Cat {cat}: ${price:,}" if isinstance(price, int) else f"  Cat {cat}: {price}")


if __name__ == "__main__":
    main()
