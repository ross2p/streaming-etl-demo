import argparse
import time
from pathlib import Path
from demo import DATA, open_database, process_message, print_report
from producer import TOPIC


def consume_file(db, path, once=False):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).touch(exist_ok=True)
    with open(path, encoding="utf-8") as log:
        while True:
            position = log.tell()
            line = log.readline()
            if line.endswith("\n"):
                process_message(db, line)
            else:
                log.seek(position)
                if once:
                    break
                time.sleep(0.2)


def consume_kafka(db, group, topic=TOPIC):
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    consumer = KafkaConsumer(
        topic, bootstrap_servers="localhost:9092", group_id=group,
        auto_offset_reset="earliest", enable_auto_commit=False,
    )
    try:
        for message in consumer:
            print(f"KAFKA: topic={message.topic}, partition={message.partition}, offset={message.offset}", flush=True)
            process_message(db, message.value.decode("utf-8", errors="replace"))
            partition = TopicPartition(message.topic, message.partition)
            consumer.commit({partition: OffsetAndMetadata(message.offset + 1, "", -1)})
    finally:
        consumer.close(autocommit=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kafka", action="store_true")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic для цієї репетиції")
    parser.add_argument("--log", default=str(DATA / "taxi-events.jsonl"))
    parser.add_argument("--schema", help="окрема схема PostgreSQL для репетиції або replay")
    parser.add_argument("--group", default="taxi-demo-v1")
    parser.add_argument("--once", action="store_true", help="дочитати файл і завершити")
    parser.add_argument("--report", action="store_true", help="лише показати підсумки")
    args = parser.parse_args()
    if args.kafka and args.once:
        parser.error("--once працює лише з файлом")
    schema = args.schema or ("taxi_kafka" if args.kafka else "taxi_local")
    db = open_database(schema)
    print("PostgreSQL, схема:", schema, flush=True)
    try:
        if args.report:
            print_report(db)
        elif args.kafka:
            consume_kafka(db, args.group, args.topic)
        else:
            consume_file(db, args.log, args.once)
    finally:
        db.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nОбробник зупинено. Дані збережено.")
