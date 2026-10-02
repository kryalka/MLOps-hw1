# Realtime Fraud Detection Service

Потоковый скоринг банковских транзакций на мошенничество


## Быстрый старт

Нужны Git и Docker с Compose V2 (`docker compose`). Python, Kafka и PostgreSQL локально ставить не нужно. Должны быть свободны порты **8501, 8080, 9095, 5433**.

```bash
git clone https://github.com/kryalka/MLOps-hw1.git
cd MLOps-hw1
docker compose up -d --build
```

Первая сборка занимает несколько минут. Проверить состояние:

```bash
docker compose ps -a
```

Ожидаемо: все сервисы запущены (`kafka` и `postgres` — `healthy`), одноразовый `kafka-setup` — `Exited (0)` (он создал топики). Если `up` вернул ошибку `unhealthy` (Kafka не успела стартовать), повторите `docker compose up -d`.

## Проверка работы

1. Создайте демо-файл из первых 500 транзакций `data/test.csv` (macOS / Linux / Git Bash):

   ```bash
   head -n 501 data/test.csv > demo.csv
   ```

   Полный `data/test.csv` (262 144 строки) тоже можно загрузить, но он обрабатывается дольше.

2. Откройте http://localhost:8501, загрузите `demo.csv` и нажмите **«Отправить demo.csv»**.

3. Дождитесь обработки: число записей в БД должно дойти до 500.

   ```bash
   docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT COUNT(*) FROM scores;"
   ```

4. В интерфейсе нажмите **«Посмотреть результаты»**: появятся 10 последних транзакций с `fraud_flag = 1` и гистограмма скоров последних 100 транзакций.

5. По желанию: http://localhost:8080 (Kafka UI) — сообщения в топиках `transactions` и `scores`.

Повторная отправка файла добавляет новые записи (у каждой транзакции новый `transaction_id`).

## Остановка

```bash
docker compose down      # остановить и удалить контейнеры; данные БД сохранятся
docker compose down -v   # то же + удалить данные БД (чистый старт)
```

## Архитектура

![Архитектура сервиса](docs/architecture.png)

`interface` на схеме показан дважды: он и отправляет транзакции, и читает результаты.

| Сервис | Назначение | Порт на хосте |
|---|---|---|
| `interface` | Streamlit: загрузка CSV, просмотр результатов | 8501 |
| `fraud_detector` | читает `transactions` → препроцессинг → CatBoost → пишет в `scores` | — |
| `scoring_writer` | читает `scores` → сохраняет в PostgreSQL | — |
| `postgres` | таблица `scores` (`transaction_id`, `score`, `fraud_flag`, `created_at`) | 5433 |
| `kafka`, `zookeeper` | брокер сообщений; `kafka-setup` создаёт топики и завершается | 9095 |
| `kafka-ui` | просмотр топиков и сообщений | 8080 |

Все сервисы работают в одной сети `ml-scorer`. Данные PostgreSQL лежат в volume `postgres_data`.

## Kafka

Топики `transactions` и `scores` (по 3 партиции) создаёт `kafka-setup`.

`transactions` — одна транзакция на сообщение; в `data` все колонки строки CSV:

```json
{"transaction_id": "550e8400-e29b-41d4-a716-446655440000",
 "data": {"transaction_time": "2019-09-14 02:46", "amount": 25.79, "...": "остальные колонки"}}
```

`scores` — результат скоринга:

```json
{"transaction_id": "550e8400-e29b-41d4-a716-446655440000", "score": 0.87, "fraud_flag": 1}
```

Оба consumer'а подтверждают offset вручную: `fraud_detector` — после публикации в `scores`, `scoring_writer` — после записи в БД. Запись идемпотентна (`UPSERT` по `transaction_id`), поэтому повторная доставка не создаёт дублей.

## Модель

`CatBoostClassifier`, инференс только на CPU. Файлы лежат в `fraud_detector/models/`: `model.cbm` (модель) и `model_meta.json` (порядок признаков и категориальные признаки; сервис сверяет их с моделью при старте).

Препроцессинг (`fraud_detector/src/preprocessing.py`): расстояние `haversine` между клиентом и мерчантом, разложение `transaction_time` на `year`, `month`, `day`, `hour`, `minute`, `weekend`, удаление `name_1`, `name_2`, `street`, приведение категориальных признаков к строкам.

Результат: `score` — вероятность фрода; `score >= 0.5` → `fraud_flag = 1`, иначе `0`.

## train/train.ipynb

**Для развёртывания не нужен** — готовая модель уже лежит в `fraud_detector/models/`. Ноутбук показывает, как были получены `model.cbm` и `model_meta.json`:

1. загрузка `data/train.csv` и та же логика препроцессинга, что в сервисе;
2. обучение `CatBoostClassifier` (200 итераций, `depth=7`, `learning_rate=0.05`, `auto_class_weights="Balanced"`, `random_seed=42`);
3. оценка на валидации (20% выборки, стратифицированно): ROC-AUC 0.9973, PR-AUC 0.857;
4. сохранение модели и метаданных в `fraud_detector/models/`.

Чтобы повторить обучение, положите `train.csv` из соревнования в `data/` (в репозитории его нет, он в `.gitignore`) и выполните ноутбук из папки `train/` (пути относительные). Файлы модели будут перезаписаны. Версии при обучении: catboost 1.2.8, pandas 2.3.3, numpy 1.26.4 — те же, что в `requirements.txt` сервиса.
