"""python3 -m unittest discover -s scripts -p 'test_*.py'  — pure parts of send_push.py."""
import unittest

import send_push as sp

LATEST = {
    "latestRound": {"id": "2026-10-2", "biddingDate": "2026-10-21T08:00:00Z", "roundLabel": "Oct 2026 Ex 2",
                    "prices": {"A": 132004, "B": 131901, "C": 92200, "D": 10778, "E": 135790}},
    "previousRound": {"id": "2026-10-1", "prices": {"A": 130001, "B": 130100, "C": 91889, "D": 11012, "E": 137000}},
}


class Decide(unittest.TestCase):
    def test_first_run_seeds_without_sending(self):
        self.assertEqual(sp.decide(LATEST, last_round=None, now_iso="2026-10-21T09:00:00Z"), ("seed", "2026-10-2"))

    def test_same_round_does_nothing(self):
        self.assertEqual(sp.decide(LATEST, "2026-10-2", "2026-10-21T09:00:00Z"), ("none", "2026-10-2"))

    def test_new_round_sends(self):
        self.assertEqual(sp.decide(LATEST, "2026-10-1", "2026-10-21T09:00:00Z"), ("send", "2026-10-2"))

    def test_stale_round_is_recorded_not_sent(self):
        # A round found days late (outage, holiday) must not buzz phones with old news.
        self.assertEqual(sp.decide(LATEST, "2026-10-1", "2026-10-24T09:00:00Z"), ("seed", "2026-10-2"))

    def test_older_round_never_sends(self):
        self.assertEqual(sp.decide(LATEST, "2026-11-1", "2026-10-21T09:00:00Z"), ("none", "2026-11-1"))


class Text(unittest.TestCase):
    def test_english(self):
        title, body = sp.message_text(LATEST, "en")
        self.assertEqual(title, "COE results · Oct 2026 Ex 2")
        self.assertEqual(body, "A $132,004 (+2,003) · B $131,901 (+1,801) · C $92,200 (+311) · "
                               "D $10,778 (−234) · E $135,790 (−1,210)")

    def test_other_languages_translate_the_title_only(self):
        en_body = sp.message_text(LATEST, "en")[1]
        for lang, start in (("zh", "COE 结果"), ("ms", "Keputusan COE"), ("ta", "COE முடிவுகள்")):
            title, body = sp.message_text(LATEST, lang)
            self.assertTrue(title.startswith(start), title)
            self.assertEqual(body, en_body)
        self.assertIn("2026年10月 第2轮", sp.message_text(LATEST, "zh")[0])

    def test_no_previous_round_shows_prices_only(self):
        body = sp.message_text({"latestRound": LATEST["latestRound"]}, "en")[1]
        self.assertEqual(body, "A $132,004 · B $131,901 · C $92,200 · D $10,778 · E $135,790")

    def test_never_advice(self):
        for lang in sp.LANGS:
            t, b = sp.message_text(LATEST, lang)
            for word in ("bid now", "buy", "should", "good time"):
                self.assertNotIn(word, (t + b).lower())


class Messages(unittest.TestCase):
    def test_one_visible_message_per_language_plus_legacy_data_only(self):
        msgs = sp.build_messages(LATEST)
        topics = [m["message"]["topic"] for m in msgs]
        self.assertEqual(topics, ["coe_results_en", "coe_results_zh", "coe_results_ms", "coe_results_ta", "coe_results"])
        for m in msgs[:4]:
            msg = m["message"]
            self.assertIn("notification", msg)
            self.assertEqual(msg["data"], {"type": "coe_results", "round": "2026-10-2"})
            self.assertEqual(msg["android"]["priority"], "high")
            self.assertEqual(msg["android"]["collapse_key"], "2026-10-2")
            self.assertEqual(msg["apns"]["headers"]["apns-priority"], "10")
            self.assertEqual(msg["apns"]["headers"]["apns-collapse-id"], "2026-10-2")
        legacy = msgs[4]["message"]
        self.assertNotIn("notification", legacy)
        self.assertNotIn("apns", legacy)
        self.assertEqual(legacy["data"]["type"], "coe_results")


if __name__ == "__main__":
    unittest.main()
