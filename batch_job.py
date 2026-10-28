import argparse
import os
import re
import time
from demo import DATABASE_URL, WINDOW_SECONDS, calculate_price

ANALYTICS_DB = "taxi_analytics"


def open_analytics():
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo
    url = os.getenv("ANALYTICS_URL")
    if not url:
        admin = psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=5)
        try:
            exists = admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (ANALYTICS_DB,)).fetchone()
            if not exists:
                admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(ANALYTICS_DB)))
        finally:
            admin.close()
        url = make_conninfo(**{**conninfo_to_dict(DATABASE_URL), "dbname": ANALYTICS_DB})
    db = psycopg.connect(url, connect_timeout=5)
    with db.transaction():
        db.execute("""CREATE TABLE IF NOT EXISTS batch_runs (
            run_id BIGSERIAL PRIMARY KEY,
            run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            source_schema TEXT NOT NULL,
            from_time BIGINT NOT NULL,
            to_time BIGINT NOT NULL,
            events_read INTEGER NOT NULL
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS window_stats (
            run_id BIGINT NOT NULL REFERENCES batch_runs,
            area TEXT NOT NULL,
            window_start BIGINT NOT NULL,
            requests INTEGER NOT NULL,
            drivers INTEGER NOT NULL,
            multiplier NUMERIC(4, 2),
            price NUMERIC(8, 2),
            PRIMARY KEY (run_id, area, window_start)
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS area_summary (
            run_id BIGINT NOT NULL REFERENCES batch_runs,
            area TEXT NOT NULL,
            requests INTEGER NOT NULL,
            drivers INTEGER NOT NULL,
            peak_multiplier NUMERIC(4, 2),
            peak_price NUMERIC(8, 2),
            PRIMARY KEY (run_id, area)
        )""")
    return db


def extract(schema, since, until):
    import psycopg
    from psycopg import sql
    source = psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=5)
    table = sql.SQL("{}.taxi_events").format(sql.Identifier(schema))
    try:
        if until == "latest":
            until = source.execute(sql.SQL("SELECT COALESCE(MAX(event_time), 0) FROM {}").format(table)).fetchone()[0]
        since = until - since
        where = sql.SQL("WHERE event_time > %s AND event_time <= %s")
        events = source.execute(sql.SQL("SELECT COUNT(*) FROM {} ").format(table) + where, (since, until)).fetchone()[0]
        windows = source.execute(sql.SQL("""SELECT area, window_start,
                   COUNT(*) FILTER (WHERE kind = 'ride_request'),
                   COUNT(DISTINCT driver_id) FILTER (WHERE kind = 'driver_available')
            FROM {} """).format(table) + where + sql.SQL(" GROUP BY area, window_start ORDER BY window_start, area"),
            (since, until)).fetchall()
        areas = source.execute(sql.SQL("""SELECT area,
                   COUNT(*) FILTER (WHERE kind = 'ride_request'),
                   COUNT(DISTINCT driver_id) FILTER (WHERE kind = 'driver_available')
            FROM {} """).format(table) + where + sql.SQL(" GROUP BY area ORDER BY area"),
            (since, until)).fetchall()
        return since, until, events, windows, areas
    except psycopg.errors.UndefinedTable:
        raise SystemExit(f"У схемі {schema} немає таблиці taxi_events. Перевір --source-schema.")
    finally:
        source.close()


def load(analytics, schema, since, until, events, windows, areas):
    stats = []
    for area, start, requests, drivers in windows:
        multiplier, price = calculate_price(requests, drivers)
        stats.append((area, start, requests, drivers, multiplier, price))
    summary = []
    for area, requests, drivers in areas:
        priced = [row for row in stats if row[0] == area and row[4] is not None]
        summary.append((area, requests, drivers,
                        max((row[4] for row in priced), default=None),
                        max((row[5] for row in priced), default=None)))
    with analytics.transaction():
        run_id = analytics.execute(
            """INSERT INTO batch_runs (source_schema, from_time, to_time, events_read)
               VALUES (%s, %s, %s, %s) RETURNING run_id""", (schema, since, until, events)).fetchone()[0]
        with analytics.cursor() as cursor:
            cursor.executemany("""INSERT INTO window_stats
                (run_id, area, window_start, requests, drivers, multiplier, price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", [(run_id, *row) for row in stats])
            cursor.executemany("""INSERT INTO area_summary
                (run_id, area, requests, drivers, peak_multiplier, peak_price)
                VALUES (%s, %s, %s, %s, %s, %s)""", [(run_id, *row) for row in summary])
    return run_id, len(stats), len(summary)


def clock(timestamp):
    return time.strftime("%H:%M:%S", time.gmtime(timestamp))


def print_run(analytics, run_id):
    run = analytics.execute("SELECT source_schema, from_time, to_time, events_read FROM batch_runs WHERE run_id = %s",
                            (run_id,)).fetchone()
    print(f"\nBATCH #{run_id}: {run[0]}, {clock(run[1])}–{clock(run[2])} UTC, прочитано подій: {run[3]}")
    print("Вікна (window_stats):")
    for area, start, requests, drivers, multiplier, price in analytics.execute(
        """SELECT area, window_start, requests, drivers, multiplier, price FROM window_stats
           WHERE run_id = %s ORDER BY window_start, area""", (run_id,)):
        cost = "немає машин" if price is None else f"x{multiplier} → {price} грн"
        print(f"  {clock(start)} | {area:8} | запити: {requests} | водії: {drivers} | {cost}")
    print("Підсумок по районах (area_summary):")
    for area, requests, drivers, peak, peak_price in analytics.execute(
        "SELECT area, requests, drivers, peak_multiplier, peak_price FROM area_summary WHERE run_id = %s ORDER BY area",
        (run_id,)):
        top = "ціни не було" if peak is None else f"пік x{peak} ({peak_price} грн)"
        print(f"  {area:8} | запити: {requests} | різних водіїв: {drivers} | {top}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-schema", default="taxi_defense_1", help="схема з taxi_events, куди пише consumer")
    parser.add_argument("--minutes", type=float, default=2, help="скільки останніх хвилин брати (за event_time)")
    parser.add_argument("--as-of", default="now", help="now, latest (остання подія в схемі) або Unix-час")
    args = parser.parse_args()
    if args.as_of not in ("now", "latest") and not re.fullmatch(r"\d+", args.as_of):
        parser.error("--as-of: now, latest або Unix-час")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", args.source_schema):
        parser.error("назва схеми: малі латинські літери, цифри, підкреслення")
    until = int(time.time()) if args.as_of == "now" else args.as_of if args.as_of == "latest" else int(args.as_of)
    since, until, events, windows, areas = extract(args.source_schema, int(args.minutes * 60), until)
    print(f"EXTRACT: {events} подій за {args.minutes:g} хв ({clock(since)}–{clock(until)} UTC), "
          f"вікон по {WINDOW_SECONDS} с: {len(windows)}")
    analytics = open_analytics()
    try:
        run_id, window_rows, area_rows = load(analytics, args.source_schema, since, until, events, windows, areas)
        print(f"LOAD: БД {ANALYTICS_DB}, рядків window_stats: {window_rows}, area_summary: {area_rows}")
        print_run(analytics, run_id)
    finally:
        analytics.close()


if __name__ == "__main__":
    main()
