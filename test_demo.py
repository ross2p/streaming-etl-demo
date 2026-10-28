import contextlib
import io
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from demo import clean_event, open_database, process_message, get_result, calculate_price
from producer import make_scenario
from consumer import consume_file


class TaxiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.schema = "test_" + uuid.uuid4().hex
        self.db = open_database(self.schema)

    def tearDown(self):
        from psycopg import sql
        self.db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.db.close()
        self.temp.cleanup()

    def send(self, event):
        with contextlib.redirect_stdout(io.StringIO()):
            return process_message(self.db, json.dumps(event))

    def test_surge_rises_and_falls_then_late_event_corrects_old_window(self):
        events = make_scenario()
        start = clean_event(events[0])["window_start"]
        prices = []
        outcomes = []
        for event in events:
            outcomes.append(self.send(event))
            prices.append(get_result(self.db, "center", start)["price"])
        self.assertEqual(prices[:8], [100, 100, 200, 300, 300, 200, 133, 100])
        self.assertEqual(outcomes.count("accepted"), 12)
        self.assertEqual(outcomes.count("duplicate"), 1)
        self.assertEqual(outcomes.count("invalid"), 1)
        self.assertEqual(prices[-1], 125)
        old = get_result(self.db, "center", start)
        self.assertEqual((old["requests"], old["drivers"]), (5, 4))
        new = get_result(self.db, "center", start + 30)
        self.assertEqual((new["requests"], new["drivers"], new["price"]), (1, 0, None))
        airport = get_result(self.db, "airport", start)
        self.assertEqual((airport["requests"], airport["drivers"], airport["price"]), (1, 0, None))

    def test_replay_after_restart_keeps_counts(self):
        events = make_scenario()
        for event in events:
            self.send(event)
        self.db.close()
        self.db = open_database(self.schema)
        for event in events:
            self.send(event)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM taxi_events").fetchone()[0], 12)

    def test_invalid_payloads_do_not_stop_etl(self):
        with contextlib.redirect_stdout(io.StringIO()):
            for raw in ["broken", "null", "[]", '{}', '{"event_id":2}']:
                self.assertEqual(process_message(self.db, raw), "invalid")
        e = make_scenario()[0]
        e["event_time"] = True
        self.assertEqual(self.send(e), "invalid")
        e["event_time"] = -1
        self.assertEqual(self.send(e), "invalid")

    def test_no_cars_and_multiplier_limits(self):
        self.assertEqual(calculate_price(5, 0), (None, None))
        self.assertEqual(calculate_price(1, 10), (1.0, 100.0))
        self.assertEqual(calculate_price(100, 1), (3.0, 300.0))

    def test_incomplete_file_line_waits_for_completion(self):
        path = Path(self.temp.name) / "events.jsonl"
        payload = json.dumps(make_scenario()[0])
        path.write_text(payload, encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()):
            consume_file(self.db, path, once=True)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM taxi_events").fetchone()[0], 0)
        with path.open("a", encoding="utf-8") as log:
            log.write("\n")
        with contextlib.redirect_stdout(io.StringIO()):
            consume_file(self.db, path, once=True)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM taxi_events").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
