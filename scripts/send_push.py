#!/usr/bin/env python3
"""Result-day push: when v1/latest.json holds a round we haven't pushed yet, send it to the apps via FCM.

    python3 scripts/send_push.py            # CI: reads FCM_SERVICE_ACCOUNT (JSON) from the environment
    python3 scripts/send_push.py --dry-run  # print what would be sent

- Topics `coe_results_<lang>` (en, zh, ms, ta) get a visible notification in that language (Steer SG 1.6+).
- Legacy topic `coe_results` gets a data-only message (Android ≤ 1.5 runs its own background check on it).
- `push/last_round.txt` remembers the last round pushed. First run (file missing) only seeds it, and a round found
  more than STALE_HOURS after bidding closed is recorded but never sent, so phones never buzz with old news.
- No secret → prints "push disabled" and exits 0 (the apps keep their local "after results" notification).
- Facts only: prices and change since the previous round. Never advice.
Spec: COESG/docs/superpowers/specs/2026-11-02-theory-depth-result-push-design.md
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LATEST = ROOT / "v1" / "latest.json"
STATE = ROOT / "push" / "last_round.txt"
LANGS = ("en", "zh", "ms", "ta")
CATS = ("A", "B", "C", "D", "E")
STALE_HOURS = 36
LEGACY_TOPIC = "coe_results"

TITLES = {
    "en": "COE results · {label}",
    "zh": "COE 结果 · {label}",
    "ms": "Keputusan COE · {label}",
    "ta": "COE முடிவுகள் · {label}",
}
MONTHS = {
    "en": "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(),
    "ms": "Jan Feb Mac Apr Mei Jun Jul Ogo Sep Okt Nov Dis".split(),
    "ta": "ஜன பிப் மார் ஏப் மே ஜூன் ஜூலை ஆக செப் அக் நவ டிச".split(),
}


def _round_parts(round_id: str) -> tuple[int, int, int]:
    y, m, n = round_id.split("-")
    return int(y), int(m), int(n)


def _key(round_id: str) -> tuple[int, int, int]:
    return _round_parts(round_id)


def round_label(round_id: str, lang: str) -> str:
    y, m, n = _round_parts(round_id)
    if lang == "zh":
        return f"{y}年{m}月 第{n}轮"
    if lang == "ms":
        return f"{MONTHS['ms'][m - 1]} {y} Bidaan {n}"
    if lang == "ta":
        return f"{MONTHS['ta'][m - 1]} {y} ஏலம் {n}"
    return f"{MONTHS['en'][m - 1]} {y} Ex {n}"


def decide(latest: dict, last_round: str | None, now_iso: str) -> tuple[str, str]:
    """('send'|'seed'|'none', round id to store)."""
    rnd = latest["latestRound"]
    rid = rnd["id"]
    if last_round is None:
        return "seed", rid
    if _key(rid) <= _key(last_round):
        return "none", last_round
    closed = dt.datetime.fromisoformat(rnd["biddingDate"].replace("Z", "+00:00"))
    now = dt.datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    if now - closed > dt.timedelta(hours=STALE_HOURS):
        return "seed", rid
    return "send", rid


def _money(n: int) -> str:
    return f"${n:,}"


def _change(n: int) -> str:
    return f"+{n:,}" if n >= 0 else f"−{abs(n):,}"


def message_text(latest: dict, lang: str) -> tuple[str, str]:
    rnd = latest["latestRound"]
    prev = (latest.get("previousRound") or {}).get("prices") or {}
    parts = []
    for c in CATS:
        p = rnd["prices"].get(c)
        if p is None:
            continue
        s = f"{c} {_money(p)}"
        if prev.get(c) is not None:
            s += f" ({_change(p - prev[c])})"
        parts.append(s)
    return TITLES[lang].format(label=round_label(rnd["id"], lang)), " · ".join(parts)


def build_messages(latest: dict) -> list[dict]:
    rid = latest["latestRound"]["id"]
    data = {"type": "coe_results", "round": rid}
    out = []
    for lang in LANGS:
        title, body = message_text(latest, lang)
        out.append({"message": {
            "topic": f"{LEGACY_TOPIC}_{lang}",
            "notification": {"title": title, "body": body},
            "data": data,
            "android": {"priority": "high", "collapse_key": rid,
                        "notification": {"channel_id": "coe_new_results", "tag": rid}},
            "apns": {"headers": {"apns-priority": "10", "apns-collapse-id": rid},
                     "payload": {"aps": {"sound": "default"}}},
        }})
    out.append({"message": {"topic": LEGACY_TOPIC, "data": data, "android": {"priority": "high"}}})
    return out


def _send(messages: list[dict], service_account: dict) -> None:
    # Imported here so the pure functions above test without these packages.
    from google.oauth2 import service_account as sa
    import google.auth.transport.requests as gtr
    import requests

    creds = sa.Credentials.from_service_account_info(
        service_account, scopes=["https://www.googleapis.com/auth/firebase.messaging"])
    creds.refresh(gtr.Request())
    url = f"https://fcm.googleapis.com/v1/projects/{service_account['project_id']}/messages:send"
    for m in messages:
        r = requests.post(url, json=m, timeout=30, headers={"Authorization": f"Bearer {creds.token}"})
        if r.status_code != 200:
            raise RuntimeError(f"FCM {m['message']['topic']}: HTTP {r.status_code} {r.text[:300]}")
        print(f"sent to {m['message']['topic']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    latest = json.loads(LATEST.read_text())
    last = STATE.read_text().strip() if STATE.exists() else None
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    action, rid = decide(latest, last or None, now)
    print(f"push: last={last} latest={latest['latestRound']['id']} → {action}")
    if action == "none":
        return 0
    if action == "send":
        msgs = build_messages(latest)
        if a.dry_run:
            print(json.dumps(msgs, indent=1, ensure_ascii=False))
            return 0
        raw = os.environ.get("FCM_SERVICE_ACCOUNT", "").strip()
        if not raw:
            print("push disabled (no FCM_SERVICE_ACCOUNT secret); round not recorded")
            return 0
        _send(msgs, json.loads(raw))
    if not a.dry_run:
        STATE.parent.mkdir(exist_ok=True)
        STATE.write_text(rid + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
