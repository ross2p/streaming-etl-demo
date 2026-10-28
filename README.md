# Демо таксі: streaming ETL із PostgreSQL

Основний показ із Kafka: **відкрий `KAFKA_DEMO_UK.md`**. Там точні команди для чистої репетиції, Kafka lag, DBeaver і replay.

Генератор → Kafka → Python streaming ETL → PostgreSQL → показники в терміналі.

Демо продовжує приклад таксі з презентації: рахує запити пасажирів і різних доступних водіїв за районами та 30-секундними вікнами. Код і формула залишаються простими. Пояснення — `CODE_EXPLAINED_UK.md`, сценарій захисту — `DEMO_SCRIPT_UK.md`.

## 1. Одноразова підготовка

Потрібні Python 3.9+ і запущений Docker Desktop. Перший запуск потребує інтернету для завантаження образів та бібліотек.

```bash
cd streaming_etl_demo
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d --wait postgres
```

PostgreSQL працює на `localhost:55432` (усередині контейнера — 5432). Навчальна база: `taxi_demo`, користувач: `taxi`, пароль: `taxi_demo`. Порт доступний лише з локального комп'ютера. Дані зберігаються в іменованому Docker-томі `postgres-data`.

Ці реквізити призначені для локального демо. Якщо маєш інший PostgreSQL, задай змінну `DATABASE_URL` перед запуском Python. База повинна існувати, а користувач — мати право створювати в ній схеми. Програма не друкує URL підключення з паролем.

## 2. Найпростіший запуск

```bash
.venv/bin/python demo.py
```

За 14 секунд відтворюється сценарій. Кожен запуск створює окрему схему `rehearsal_...` у PostgreSQL та папку `data/rehearsal_...` із сирими повідомленнями. Назва схеми друкується на початку. Попередні результати не видаляються.

Швидка перевірка без пауз:

```bash
.venv/bin/python demo.py --fast
```

Це однопроцесна репетиція з реальною PostgreSQL.

## 3. Що покажеш викладачеві

| Стан першого вікна center | Коефіцієнт | Умовна ціна |
|---|---:|---:|
| 1 запит, 1 водій | 1.00 | 100 грн |
| 2 запити, 1 водій | 2.00 | 200 грн |
| 4 запити, 1 водій | 3.00, верхня межа | 300 грн |
| 4 запити, 2 водії | 2.00 | 200 грн |
| 4 запити, 3 водії | 1.33 | 133 грн |
| 4 запити, 4 водії | 1.00 | 100 грн |
| Запізнілий п'ятий запит, 4 водії | 1.25 | 125 грн |

Один сценарій: 14 повідомлень, 12 збережених подій, один дублікат і одна помилка. Підсумок першого вікна center: **5 запитів, 4 різних водії, x1.25, 125 грн**. В airport і наступному center-вікні — по одному запиту й нуль водіїв; ціну не пропонуємо.

Час у терміналі — UTC. Сценарій має синтетичні timestamps, щоб показати перехід між вікнами та запізнення без довгого очікування.

## 4. Два термінали: файл + PostgreSQL

У кожному терміналі перейди до папки проєкту.

Термінал 1 — consumer очікує події:

```bash
.venv/bin/python consumer.py
```

Термінал 2 — producer надсилає 14 повідомлень:

```bash
.venv/bin/python producer.py
```

Журнал — `data/taxi-events.jsonl`, результати — таблиця `taxi_local.taxi_events` у PostgreSQL.

Зупини лише consumer через Ctrl+C, дай producer завершити сценарій, потім знову запусти consumer. Він перечитає файл, пропустить збережені event_id і додасть решту.

Дочитати файл та завершитися або показати звіт:

```bash
.venv/bin/python consumer.py --once
.venv/bin/python consumer.py --report
```

Для нової чистої репетиції використовуй новий файл та нову схему. У різних терміналах:

```bash
.venv/bin/python consumer.py --log data/show2.jsonl --schema show2
.venv/bin/python producer.py --log data/show2.jsonl
```

Повторний producer дописує нові події. Точні числа одного сценарію гарантуються лише на чистому файлі/схемі або при запуску `demo.py`.

## 5. Як показати дані безпосередньо у PostgreSQL

Відкрити psql усередині контейнера:

```bash
docker compose exec postgres psql -U taxi -d taxi_demo
```

Список схем:

```sql
\dn
```

Для двотермінального файлового режиму:

```sql
SELECT * FROM taxi_local.taxi_events ORDER BY event_time, event_id;
```

Для однопроцесної репетиції заміни `taxi_local` на назву `rehearsal_...`, яку надрукувала програма. Для Kafka стандартна схема — `taxi_kafka`.

Запит для показу агрегатів:

```sql
SELECT area,
       to_timestamp(window_start) AT TIME ZONE 'UTC' AS window_utc,
       COUNT(*) FILTER (WHERE kind = 'ride_request') AS requests,
       COUNT(DISTINCT driver_id) FILTER (WHERE kind = 'driver_available') AS drivers
FROM taxi_local.taxi_events
GROUP BY area, window_start
ORDER BY window_start, area;
```

Вихід із psql: `\q`. Можна також підключити DBeaver/pgAdmin за реквізитами з першого розділу; вони не потрібні для запуску демо.

## 6. Справжня Kafka + PostgreSQL

Після встановлення залежностей:

```bash
docker compose up -d --wait postgres kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic taxi-events --partitions 3 --replication-factor 1
```

Термінал 1:

```bash
.venv/bin/python consumer.py --kafka
```

Термінал 2:

```bash
.venv/bin/python producer.py --kafka
```

Звіт із PostgreSQL:

```bash
.venv/bin/python consumer.py --schema taxi_kafka --report
```

Kafka key — нормалізований район. Порядок усередині його партиції зберігається; порядок між районами може відрізнятися. Це не змінює підсумків нашої моделі.

Повторне читання історії в окрему схему:

```bash
.venv/bin/python consumer.py --kafka --group taxi-replay-1 --schema taxi_replay_1
```

Потрібні **нова consumer group і нова схема**. Стара група пам'ятає offsets, тому порожня схема зі старою групою не відновить усю історію. Replay можливий лише для подій, які ще зберігаються в Kafka.

Зупинка: Ctrl+C у Python-процесах, потім `docker compose down`. Звичайний `down` зберігає іменовані томи. Не додавай `-v`, якщо потрібні дані.

## 7. Межі моделі

Район уже заданий у події: GPS і геокодування не реалізовані. Рахуємо водіїв, які повідомили про доступність у вікні, а не точний поточний стан автопарку. Подій зайнятості, скасування чи переходу між районами немає.

Вікна tumbling по 30 секунд; на слайді — hopping на 5 хвилин. Запізніла подія уточнює своє старе вікно. Watermark і закриття вікон прибрано для простоти.

Один Python worker, один PostgreSQL і один Kafka broker. Це не демонстрація CAP, високої доступності, Lambda/Lakehouse чи ML-прогнозу ETA. Ціна — навчальна формула, реальних платежів немає.

Повторний запис ідемпотентний за event_id. Повторення виводу в термінал можливе, а загальна exactly-once гарантія для інших систем не заявляється. Невалідні події залишаються у вхідному журналі та друкуються як INVALID, але не завантажуються в таблицю.

## 8. Перевірки

Потрібен запущений PostgreSQL:

```bash
.venv/bin/python -m unittest -v
```

Тести створюють окремі схеми `test_...` і видаляють лише їх після перевірки. Перевіряють формулу, дублікати, повторне підключення, запізнення, райони/вікна та неповні рядки журналу.

Для наскрізної перевірки Kafka → PostgreSQL, відновлення та replay є `.venv/bin/python check_kafka.py`.

Документація драйвера: https://www.psycopg.org/psycopg3/docs/basic/usage.html
Приклад викладача: https://github.com/babanomania/etl-streaming
