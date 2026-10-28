import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from psycopg import sql
from demo import DATA, open_database, get_result


def wait_until(condition, worker=None, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if worker is not None and worker.poll() is not None:
            raise RuntimeError("Consumer завершився: дивись consumer.log у папці перевірки")
        if condition():
            return
        time.sleep(0.2)
    raise TimeoutError("Не дочекалися результату за 45 секунд")


def stop(worker):
    if worker is not None and worker.poll() is None:
        worker.send_signal(signal.SIGINT)
        try:
            worker.wait(timeout=15)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait()


def main():
    name = "check_" + uuid.uuid4().hex[:8]
    topic, group, replay_group = name, name + "_group", name + "_replay_group"
    folder = DATA / name
    folder.mkdir(parents=True)
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
    reader = KafkaConsumer(bootstrap_servers="localhost:9092", enable_auto_commit=False)
    db = open_database(name)
    replay = open_database(name + "_replay")
    worker = producer = None
    logs = []
    created = False

    def start_consumer(schema, consumer_group):
        log = (folder / (schema + "-consumer.log")).open("w")
        logs.append(log)
        return subprocess.Popen([sys.executable, "consumer.py", "--kafka", "--topic", topic,
                                 "--group", consumer_group, "--schema", schema],
                                stdout=log, stderr=subprocess.STDOUT)

    def count(connection):
        return connection.execute("SELECT COUNT(*) FROM taxi_events").fetchone()[0]

    try:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        created = True
        worker = start_consumer(name, group)
        log = (folder / "producer.log").open("w")
        logs.append(log)
        producer = subprocess.Popen([sys.executable, "producer.py", "--kafka", "--topic", topic],
                                    stdout=log, stderr=subprocess.STDOUT)
        wait_until(lambda: count(db) >= 2, worker)
        stop(worker)
        paused_count = count(db)
        assert producer.wait(timeout=30) == 0, "Producer failed; see producer.log"
        assert count(db) == paused_count, "Результат змінився без consumer"
        partitions = [TopicPartition(topic, p) for p in range(3)]
        ends = reader.end_offsets(partitions)
        assert sum(ends.values()) == 14
        offsets = admin.list_consumer_group_offsets(group)
        lag = sum(end - (offsets[tp].offset if tp in offsets else 0) for tp, end in ends.items())
        assert lag > 0
        print(f"PASS: consumer stopped, PostgreSQL rows={paused_count}, Kafka lag={lag}", flush=True)

        worker = start_consumer(name, group)
        wait_until(lambda: count(db) == 12, worker)
        def all_committed():
            offsets = admin.list_consumer_group_offsets(group)
            return all(tp in offsets and offsets[tp].offset == end for tp, end in ends.items())
        wait_until(all_committed, worker)
        stop(worker)
        start = db.execute("SELECT MIN(window_start) FROM taxi_events").fetchone()[0]
        result = get_result(db, "center", start)
        assert (result["requests"], result["drivers"], result["price"]) == (5, 4, 125)
        print("PASS: restart caught up, lag=0; 5 requests / 4 drivers / 125 UAH", flush=True)

        worker = start_consumer(name + "_replay", replay_group)
        wait_until(lambda: count(replay) == 12, worker)
        stop(worker)
        query = "SELECT * FROM taxi_events ORDER BY event_id"
        assert db.execute(query).fetchall() == replay.execute(query).fetchall()
        print("PASS: new group replay produced identical PostgreSQL rows", flush=True)
        print("Logs:", folder, flush=True)
    finally:
        stop(worker)
        stop(producer)
        for log in logs:
            log.close()
        reader.close()
        db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))
        replay.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name + "_replay")))
        db.close()
        replay.close()
        if created:
            admin.delete_topics([topic])
        admin.close()


if __name__ == "__main__":
    main()
