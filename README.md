# BigDataSpark

Лабораторная работа №2 по курсу «Анализ больших данных».

ETL-пайплайн на Apache Spark: исходные CSV-данные загружаются в PostgreSQL,
Spark строит из них модель **«звезда»** (тоже в PostgreSQL), а затем на основе
звезды формирует **6 аналитических витрин в ClickHouse**.

```
CSV → PostgreSQL (mock_data) → Spark → PostgreSQL (звезда) → Spark → ClickHouse (витрины)
```

## Структура репозитория

```
BigDataSpark/
├── docker-compose.yml            — PostgreSQL, ClickHouse, Spark (master + worker)
├── download_jars.sh / .ps1       — загрузка JDBC-драйверов в jars/
├── исходные данные/              — 10 файлов MOCK_DATA*.csv
├── jars/                         — JDBC-драйверы (не коммитятся)
├── init/01_load_raw.sql          — загрузка CSV в сырую таблицу mock_data
├── spark/
│   ├── build_star.py             — построение звезды в PostgreSQL
│   └── clickhouse_reports.py     — построение витрин в ClickHouse
├── sql/checks.sql                — проверочные запросы к звезде
└── report.md                     — отчёт по работе
```

Каталог `исходные данные/` монтируется в контейнер PostgreSQL как `/data`.

## Модель «звезда»

В центре — таблица фактов `fact_sales` (зерно: одна строка CSV = одна продажа).
Вокруг неё шесть измерений, ключи которых лежат прямо в факте:

```
      dim_customer   dim_seller   dim_product
               \         |         /
                \        |        /
   dim_store ───────  fact_sales  ─────── dim_supplier
                /        |        \
               /         |         \
                       dim_pet
```

Атрибуты (категория и бренд товара, город и страна магазина/поставщика и т.п.)
хранятся прямо в измерениях — это отличает «звезду» от «снежинки».

## Витрины в ClickHouse (база `marts`)

| Таблица | Содержание |
|---|---|
| `report_product_sales`  | продажи по товарам: количество, выручка, средний рейтинг, отзывы |
| `report_customer_sales` | продажи по клиентам: сумма покупок, средний чек, страна |
| `report_time_sales`     | продажи по месяцам и годам: выручка, средний размер заказа |
| `report_store_sales`    | продажи по магазинам: выручка, средний чек, город и страна |
| `report_supplier_sales` | продажи по поставщикам: выручка, средняя цена товара, страна |
| `report_product_quality`| качество товара: рейтинг, отзывы, корреляция рейтинга и продаж |

## 1. Подготовка

Исходные CSV уже лежат в каталоге `исходные данные/`. Нужно скачать JDBC-драйверы:

```powershell
powershell -ExecutionPolicy Bypass -File .\download_jars.ps1
```
```bash
./download_jars.sh        # Linux / WSL
```

В `jars/` должны появиться `postgresql-42.7.4.jar` и `clickhouse-jdbc-0.6.0-all.jar`.

## 2. Запуск контейнеров

```bash
docker compose up -d
```

Проверка загрузки CSV в PostgreSQL (ожидается `10000`):

```bash
docker exec -it sds_postgres psql -U postgres -d spark_dwh -c "SELECT COUNT(*) FROM mock_data;"
```

## 3. Построение звезды в PostgreSQL

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/drivers/postgresql-42.7.4.jar \
  --driver-class-path /opt/spark/drivers/postgresql-42.7.4.jar \
  --conf spark.driver.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar \
  --conf spark.executor.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar \
  /opt/spark/app/build_star.py
```

Проверка (ожидается `source_rows = 10000`, `fact_rows = 10000`, `missing_rows = 0`):

```bash
docker exec -it sds_postgres psql -U postgres -d spark_dwh -c "SELECT * FROM view_load_quality;"
```

## 4. Построение витрин в ClickHouse

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/drivers/postgresql-42.7.4.jar,/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar \
  --driver-class-path /opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar \
  --conf spark.driver.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar \
  --conf spark.executor.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar \
  /opt/spark/app/clickhouse_reports.py
```

> На Windows команду удобнее выполнять в одну строку из `cmd` или PowerShell.

## 5. Проверка витрин в ClickHouse

```bash
docker exec -it sds_clickhouse clickhouse-client --user user --password password \
  --query "SELECT product_name, category, total_quantity, revenue FROM marts.report_product_sales ORDER BY total_quantity DESC LIMIT 10 FORMAT Pretty;"
```

```bash
docker exec -it sds_clickhouse clickhouse-client --user user --password password \
  --query "SELECT year, month, revenue, avg_order_value FROM marts.report_time_sales ORDER BY year, month FORMAT Pretty;"
```

Остальные витрины проверяются аналогично (`report_customer_sales`,
`report_store_sales`, `report_supplier_sales`, `report_product_quality`).

## 6. Перезапуск с нуля

```bash
docker compose down -v && docker compose up -d
```

После этого заново выполнить шаги 3 и 4.

## Автор

Скрипачёв Ф. М., группа М8О-409Б-23.
