import argparse
import json
import os
import re
import time
import uuid
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"
WINDOW_SECONDS = 30
BASE_PRICE = 100
AREAS = {"center", "station", "airport"}
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://taxi:taxi_demo@localhost:55432/taxi_demo"
)


def open_database(schema="taxi_local"):
    import psycopg
    from psycopg import sql
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
        raise ValueError("назва схеми: малі латинські літери, цифри, підкреслення")
    db = psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=5)
    db.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    db.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    db.execute("""CREATE TABLE IF NOT EXISTS taxi_events (
        event_id TEXT PRIMARY KEY,
        event_time BIGINT NOT NULL,
        area TEXT NOT NULL,
        kind TEXT NOT NULL,
        driver_id TEXT,
        window_start BIGINT NOT NULL
    )""")
    return db


def clean_event(event):
    if not isinstance(event, dict):
        raise ValueError("подія повинна бути JSON-об'єктом")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("немає event_id")
    area = event.get("area")
    if not isinstance(area, str) or area.strip().lower() not in AREAS:
        raise ValueError("невідомий район")
    kind = event.get("kind")
    if kind not in ("ride_request", "driver_available"):
        raise ValueError("невідомий тип події")
    event_time = event.get("event_time")
    if type(event_time) is not int or not 0 <= event_time <= 32503680000:
        raise ValueError("event_time має бути цілим Unix-часом у підтримуваному діапазоні")
    driver_id = event.get("driver_id")
    if kind == "driver_available":
        if not isinstance(driver_id, str) or not driver_id.strip():
            raise ValueError("для водія потрібен driver_id")
        driver_id = driver_id.strip()
    else:
        driver_id = None
    return {
        "event_id": event_id.strip(),
        "event_time": event_time,
        "area": area.strip().lower(),
        "kind": kind,
        "driver_id": driver_id,
        "window_start": event_time // WINDOW_SECONDS * WINDOW_SECONDS,
    }


def save_event(db, event):
    cursor = db.execute("""INSERT INTO taxi_events
        (event_id, event_time, area, kind, driver_id, window_start)
        VALUES (%(event_id)s, %(event_time)s, %(area)s, %(kind)s,
                %(driver_id)s, %(window_start)s)
        ON CONFLICT (event_id) DO NOTHING""", event)
    return cursor.rowcount == 1


def calculate_price(requests, drivers):
    if drivers == 0:
        return None, None
    multiplier = round(min(3.0, max(1.0, requests / drivers)), 2)
    return multiplier, round(BASE_PRICE * multiplier, 2)


def get_result(db, area, window_start):
    requests, drivers = db.execute("""
        SELECT COUNT(CASE WHEN kind = 'ride_request' THEN 1 END),
               COUNT(DISTINCT CASE WHEN kind = 'driver_available' THEN driver_id END)
        FROM taxi_events WHERE area = %s AND window_start = %s
    """, (area, window_start)).fetchone()
    multiplier, price = calculate_price(requests, drivers)
    return {"area": area, "window_start": window_start, "requests": requests,
            "drivers": drivers, "multiplier": multiplier, "price": price}


def print_result(result):
    start = time.strftime("%H:%M:%S", time.gmtime(result["window_start"]))
    price = "немає машин" if result["price"] is None else (
        f"x{result['multiplier']:.2f} → {result['price']:.2f} грн")
    print(f"{start} UTC | {result['area']:7} | запити: {result['requests']} | "
          f"водії: {result['drivers']} | {price}", flush=True)


def process_message(db, message):
    try:
        event = clean_event(json.loads(message))
    except (ValueError, TypeError) as error:
        print(f"INVALID: {error}", flush=True)
        return "invalid"
    if not save_event(db, event):
        print(f"DUPLICATE: {event['event_id']} — пропускаємо", flush=True)
        return "duplicate"
    print_result(get_result(db, event["area"], event["window_start"]))
    return "accepted"


def print_report(db):
    for area, start in db.execute(
        "SELECT DISTINCT area, window_start FROM taxi_events ORDER BY window_start, area"
    ).fetchall():
        print_result(get_result(db, area, start))


def main():
    from producer import make_scenario
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fast", action="store_true", help="без пауз між подіями")
    args = parser.parse_args()
    schema = "rehearsal_" + uuid.uuid4().hex[:8]
    folder = DATA / schema
    folder.mkdir(parents=True, exist_ok=True)
    db = open_database(schema)
    print("РЕПЕТИЦІЯ БЕЗ KAFKA. PostgreSQL, схема:", schema, flush=True)
    print("Сирі події:", folder / "events.jsonl", flush=True)
    try:
        with (folder / "events.jsonl").open("w", encoding="utf-8") as log:
            for event in make_scenario():
                message = json.dumps(event, ensure_ascii=False)
                log.write(message + "\n")
                log.flush()
                print("\nEXTRACT:", message, flush=True)
                process_message(db, message)
                if not args.fast:
                    time.sleep(1)
        print("\nПІДСУМОК")
        print_report(db)
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nЗупинено. Результати збережені у PostgreSQL, сирі події — у data/.")
