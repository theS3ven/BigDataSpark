# BigDataSpark — Лабораторная работа №2

Анализ больших данных — ETL-пайплайн на Apache Spark.

---

## Инструкция по запуску и проверке

### Требования

- Docker Desktop (версия 24+)
- DBeaver (для просмотра баз данных)
- Свободные порты: `5434` (PostgreSQL), `8123` / `9000` (ClickHouse), `8080` / `7077` (Spark)

---

### Шаг 1. Запуск окружения

Из папки `BigDataSpark` выполнить:

```bash
docker-compose up -d --build
```

Команда автоматически:
- собирает образ Apache Spark с JDBC-драйвером PostgreSQL
- запускает PostgreSQL и загружает все 10 CSV-файлов (10 000 строк) в таблицу `mock_data`
- запускает ClickHouse
- запускает Spark Master и Spark Worker

Дождаться, пока все контейнеры перейдут в статус `healthy`:

```bash
docker-compose ps
```

Ожидаемый результат — 4 контейнера в статусе `running`:

```
bigdata_spark_postgres    running (healthy)
bigdata_spark_clickhouse  running (healthy)
spark_master              running
spark_worker              running
```

Spark UI доступен в браузере: **http://localhost:8080**

---

### Шаг 2. Запуск Spark ETL-джобов

```bash
./run_etl.sh
```

Скрипт запускает два джоба последовательно.

**Джоб 1** — `etl_star_schema.py`:
читает `mock_data` из PostgreSQL, строит модель «звезда» и записывает обратно в PostgreSQL.

**Джоб 2** — `etl_clickhouse.py`:
читает схему-звезду из PostgreSQL, вычисляет 6 аналитических витрин и записывает в ClickHouse.

В конце должно появиться:
```
=== Все ETL-джобы выполнены ===
```

Если нужно запустить джобы по отдельности:

```bash
# Только звезда в PostgreSQL
docker exec spark_master /opt/spark/bin/spark-submit \
  --master local[*] /opt/spark-jobs/etl_star_schema.py

# Только отчёты в ClickHouse
docker exec spark_master /opt/spark/bin/spark-submit \
  --master local[*] /opt/spark-jobs/etl_clickhouse.py
```

---

### Шаг 3. Проверка PostgreSQL (схема «звезда»)

Подключение в DBeaver:
| Параметр | Значение |
|----------|----------|
| Host | `localhost` |
| Port | `5434` |
| Database | `petstore` |
| User | `bigdata` |
| Password | `bigdata123` |

#### Проверка источника и измерений

```sql
-- Источник: ровно 10 000 строк
SELECT COUNT(*) FROM mock_data;

-- Измерения
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_seller;
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM dim_store;
SELECT COUNT(*) FROM dim_supplier;
SELECT COUNT(*) FROM dim_date;

-- Факты
SELECT COUNT(*) FROM fact_sales;
```

#### Проверка работы схемы-звезды (join всех таблиц)

```sql
SELECT
    dc.first_name || ' ' || dc.last_name  AS customer,
    dp.product_name,
    dp.category,
    ds.store_name,
    ds.store_city,
    dd.full_date,
    f.quantity,
    f.total_price
FROM fact_sales      f
JOIN dim_customer   dc ON f.customer_id = dc.customer_id
JOIN dim_product    dp ON f.product_id  = dp.product_id
JOIN dim_store      ds ON f.store_id    = ds.store_id
JOIN dim_date       dd ON f.date_id     = dd.date_id
LIMIT 20;
```

---

### Шаг 4. Проверка ClickHouse (аналитические витрины)

Подключение в DBeaver:
| Параметр | Значение |
|----------|----------|
| Host | `localhost` |
| Port | `8123` |
| Database | `petstore_reports` |
| User | `bigdata` |
| Password | `bigdata123` |

#### Витрина 1. Продажи по продуктам

```sql
-- Топ-10 самых продаваемых продуктов
SELECT product_name, category, total_quantity, total_revenue, avg_rating, review_count
FROM report_product_sales
ORDER BY quantity_rank
LIMIT 10;

-- Общая выручка по категориям
SELECT category, SUM(total_revenue) AS category_revenue
FROM report_product_sales
GROUP BY category
ORDER BY category_revenue DESC;
```

#### Витрина 2. Продажи по клиентам

```sql
-- Топ-10 клиентов по сумме покупок
SELECT full_name, country, total_purchases, avg_check, purchase_count
FROM report_customer_sales
ORDER BY purchase_rank
LIMIT 10;

-- Распределение клиентов по странам
SELECT country, COUNT(*) AS customer_count
FROM report_customer_sales
GROUP BY country
ORDER BY customer_count DESC;
```

#### Витрина 3. Продажи по времени

```sql
-- Месячные тренды продаж
SELECT year, month, total_revenue, total_quantity, sale_count, avg_order_size
FROM report_time_sales
ORDER BY year, month;

-- Годовая выручка
SELECT year, SUM(total_revenue) AS annual_revenue
FROM report_time_sales
GROUP BY year
ORDER BY year;
```

#### Витрина 4. Продажи по магазинам

```sql
-- Топ-5 магазинов по выручке
SELECT store_name, store_city, store_country, total_revenue, avg_check, sale_count
FROM report_store_sales
ORDER BY revenue_rank
LIMIT 5;

-- Распределение продаж по странам
SELECT store_country, SUM(total_revenue) AS country_revenue
FROM report_store_sales
GROUP BY store_country
ORDER BY country_revenue DESC;
```

#### Витрина 5. Продажи по поставщикам

```sql
-- Топ-5 поставщиков по выручке
SELECT supplier_name, supplier_country, total_revenue, avg_price, sale_count
FROM report_supplier_sales
ORDER BY revenue_rank
LIMIT 5;

-- Средняя цена по странам поставщиков
SELECT supplier_country, AVG(avg_price) AS mean_price
FROM report_supplier_sales
GROUP BY supplier_country
ORDER BY mean_price DESC;
```

#### Витрина 6. Качество продукции

```sql
-- Продукты с наивысшим рейтингом
SELECT product_name, category, rating, reviews, total_quantity, total_revenue
FROM report_quality
ORDER BY rating_rank
LIMIT 10;

-- Продукты с наибольшим количеством отзывов
SELECT product_name, category, reviews, rating, total_quantity
FROM report_quality
ORDER BY reviews_rank
LIMIT 10;
```

---

### Структура проекта

```
BigDataSpark/
├── docker-compose.yml          # PostgreSQL + Spark + ClickHouse
├── run_etl.sh                  # скрипт запуска обоих джобов
├── исходные данные/            # 10 CSV-файлов (10 000 строк)
├── sql/
│   ├── 01_create_raw.sql       # DDL таблицы mock_data
│   ├── 02_import_raw.sql       # загрузка CSV в PostgreSQL
│   └── 03_clickhouse_ddl.sql   # DDL таблиц ClickHouse (справочно)
└── spark/
    ├── Dockerfile              # apache/spark:3.5.5 + PostgreSQL JDBC
    ├── etl_star_schema.py      # Джоб 1: mock_data → схема-звезда в PostgreSQL
    └── etl_clickhouse.py       # Джоб 2: схема-звезда → 6 витрин в ClickHouse
```

### Используемые технологии

| Компонент | Технология | Версия |
|-----------|-----------|--------|
| ETL-фреймворк | Apache Spark (PySpark) | 3.5.5 |
| Оперативное хранилище | PostgreSQL | 16 |
| Аналитическое хранилище | ClickHouse | 24.3 |
| Оркестрация | Docker Compose | — |

---

## Описание задания

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

Что необходимо сделать?

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
2. Витрина продаж по клиентам
3. Витрина продаж по времени
4. Витрина продаж по магазинам
5. Витрина продаж по поставщикам
6. Витрина качества продукции

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)** — реализовано
2. Cassandra (опционально)
3. Neo4J (опционально)
4. MongoDB (опционально)
5. Valkey (опционально)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)
