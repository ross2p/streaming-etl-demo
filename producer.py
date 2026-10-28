import argparse
import json
import time
import uuid
from demo import DATA, WINDOW_SECONDS

TOPIC = "taxi-events"


def make_scenario():
    start = int(time.time()) // WINDOW_SECONDS * WINDOW_SECONDS
    run_id = uuid.uuid4().hex[:8]

    def event(number, seconds, kind, driver=None, area=" Center "):
        return {"event_id": f"{run_id}-{number}", "event_time": start + seconds,
                "area": area, "kind": kind, "driver_id": driver}

    request = event(2, 2, "ride_request")
    return [
        event(1, 1, "driver_available", "driver-1"),
        request,
        event(3, 3, "ride_request"),
        event(4, 4, "ride_request"),
        event(5, 5, "ride_request"),
        event(6, 6, "driver_available", "driver-2"),
        event(7, 7, "driver_available", "driver-3"),
        event(8, 8, "driver_available", "driver-4"),
        dict(request),
        event(9, 9, "driver_available", "driver-4"),
        event(10, 10, "ride_request", area="unknown"),
        event(11, 11, "ride_request", area="airport"),
        event(12, 31, "ride_request"),
        event(13, 12, "ride_request"),
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka", action="store_true")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic для цієї репетиції")
    parser.add_argument("--log", default=str(DATA / "taxi-events.jsonl"))
    args = parser.parse_args()
    producer = None
    log = None
    try:
        if args.kafka:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers="localhost:9092", acks="all",
                                     retries=5, max_in_flight_requests_per_connection=1)
        else:
            from pathlib import Path
            Path(args.log).parent.mkdir(parents=True, exist_ok=True)
            log = open(args.log, "a", encoding="utf-8")
        for event in make_scenario():
            message = json.dumps(event, ensure_ascii=False)
            if producer is not None:
                key = event["area"].strip().lower().encode("utf-8")
                sent = producer.send(args.topic, key=key, value=message.encode("utf-8")).get(timeout=30)
                print(f"KAFKA: topic={sent.topic}, partition={sent.partition}, offset={sent.offset}", flush=True)
            else:
                log.write(message + "\n")
                log.flush()
            print("SEND:", message, flush=True)
            time.sleep(1)
    finally:
        if producer is not None:
            producer.close(timeout=10)
        if log is not None:
            log.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nГенератор зупинено.")
