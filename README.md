# Trino + Iceberg (Nessie)

## Что уже есть
- **docker-compose** c (Trino 472, MinIO, Postgres, ClickHouse, Nessie, Iceberg, SQLPad).
- В Trino настроены каталоги (имена важны — используем их в SQL):
  - `clickhouse`
  - `postgres`
  - `iceberg-jdbc`  — JDBC‑каталог Iceberg (метаданные в Postgres)
  - `iceberg-nessie-main` — Iceberg (каталог Nessie, ветка `main` (ветка by default))
  - `iceberg-nessie-dev` — Iceberg (каталог Nessie, ветка `dev` (нужно будет создать))
  - `metastore` - хранилище метаданных для iceberg c jdbc и nessie

---

## 0) Предподготовка (один раз)
1. Поднимите всё:
   ```bash
   docker-compose -f docker-compose.yaml up -d
   docker-compose -f docker-compose-trino.yaml up -d
   docker-compose -f docker-compose-nc.yaml up -d
   ```
   - `docker-compose.yaml` - postgres, clickhouse, minio, nessie, sqlpad
   - `docker-compose-trino.yaml` - trino coordinator + 2 workers
   - `docker-compose-nc.yaml` - nessie-cli для подключения к nessie и работы с ветками
2. Создайте **bucket** `iceberg` в MinIO:
   - Откройте MinIO Console: http://localhost:9001 (логин/пароль: `minioadmin`/`minioadmin`).
   - Создайте bucket с именем **`iceberg`** (публичным делать не требуется).

3. (Опционально) Если таблицы `iceberg_tables`/`iceberg_namespace_properties` в БД `metastore` не создались — выполните в контейнере Postgres (обычно ваш entrypoint уже это делает):
   ```bash
   docker exec -it postgres psql -U postgres -d metastore -c "
   CREATE TABLE IF NOT EXISTS iceberg_tables (
     catalog_name VARCHAR(255) NOT NULL,
     table_namespace VARCHAR(255) NOT NULL,
     table_name VARCHAR(255) NOT NULL,
     metadata_location VARCHAR(1000),
     previous_metadata_location VARCHAR(1000),
     PRIMARY KEY (catalog_name, table_namespace, table_name)
   );
   CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
     catalog_name VARCHAR(255) NOT NULL,
     namespace VARCHAR(255) NOT NULL,
     property_key VARCHAR(255),
     property_value VARCHAR(1000),
     PRIMARY KEY (catalog_name, namespace, property_key)
   );"
   ```

4. Откройте **SQLPad**: http://localhost:3000 (логин/пароль: `admin`/`admin`) и добавьте подключение к Trino (Host: `http://trino-coordinator:8080` из контейнера или `http://localhost:8080` с хоста).

---

## 1) Создаём схемы и таблицы‑источники
Ниже — минимальные наборы данных в **Postgres**, **ClickHouse** и справочник в **Iceberg‑JDBC**. Выполняйте SQL из **Trino** (кроме DDL ClickHouse — его удобнее выполнить в самом ClickHouse).

### 1.1 Postgres (через Trino)
```sql
-- Схема public обычно уже есть. Создадим таблицу и данные.
DROP TABLE IF EXISTS postgres.public.customer;
CREATE TABLE postgres.public.customer (
    id          INT,
    name        VARCHAR,
    email       VARCHAR,
    created_at  TIMESTAMP,
    country_id  INT
);

INSERT INTO postgres.public.customer (id, name, email, created_at, country_id) VALUES
 (1, 'Alice',   'alice@example.com',   TIMESTAMP '2025-03-12 10:00:00', 1),
 (2, 'Bob',     'bob@example.com',     TIMESTAMP '2025-03-12 10:05:00', 2),
 (3, 'Charlie', 'charlie@example.com', TIMESTAMP '2025-03-12 10:10:00', 3);
```

### 1.2 ClickHouse (в самом ClickHouse)
> **Почему не через Trino?** Через Trino вы будете ограничены возможностями коннектора. Для простоты ниже — нативный SQL ClickHouse с движком `MergeTree`.

```sql
# Создание таблицы и данных нативно в ClickHouse
CREATE TABLE IF NOT EXISTS default.product (
  id UInt32,
  customer_id UInt32,
  product_name String,
  price Float32,
  country_id UInt32
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO default.product VALUES
(1, 1, 'Laptop', 1200.50, 1),
(2, 2, 'Phone',   800.75, 2),
(3, 1, 'Tablet',  450.99, 3);
```

Проверка из Trino:
```sql
SELECT * FROM clickhouse.default.product ORDER BY id;
```

### 1.3 Iceberg‑JDBC (через Trino) — справочник стран
```sql
CREATE SCHEMA IF NOT EXISTS "iceberg-jdbc".default;

DROP TABLE IF EXISTS "iceberg-jdbc".default.country;
CREATE TABLE "iceberg-jdbc".default.country (
    id   INT,
    name VARCHAR
);

INSERT INTO "iceberg-jdbc".default.country (id, name) VALUES
 (1, 'USA'),
 (2, 'China'),
 (3, 'Germany');
```

### 1.4 Iceberg (Nessie, main) — целевая схема для результата
```sql
CREATE SCHEMA IF NOT EXISTS "iceberg-nessie-main".analytics;
```

---

## 2) JOIN и загрузка результата в Iceberg (Nessie)
Создадим таблицу **CTAS** в каталоге `iceberg-nessie-main`, куда поместим результат объединения Postgres + ClickHouse + Iceberg‑JDBC.

```sql
DROP TABLE IF EXISTS "iceberg-nessie-main".analytics.products_enriched;

CREATE TABLE "iceberg-nessie-main".analytics.products_enriched AS
SELECT
    c.id                               AS customer_id,
    c.name                             AS customer_name,
    c.email,
    co.name                            AS customer_country,
    p.id                               AS product_id,
    p.product_name,
    p.price,
    p_co.name                          AS product_country,
    c.created_at,
    CURRENT_TIMESTAMP                  AS ingested_at
FROM postgres.public.customer c
JOIN clickhouse.default.product p   ON c.id = p.customer_id
JOIN "iceberg-jdbc".default.country co   ON c.country_id = co.id
JOIN "iceberg-jdbc".default.country p_co ON p.country_id = p_co.id;
```

Проверка:
```sql
SELECT * FROM "iceberg-nessie-main".analytics.products_enriched ORDER BY product_id;
```

---

## 3) История и тайм‑тревел Iceberg
Iceberg ведёт метаданные в виде специальных таблиц. Полезные из них: `$snapshots` и `$history`.

```sql
-- Снимки (snapshot_id пригодится для запросов во времени)
SELECT snapshot_id, committed_at, operation
FROM "iceberg-nessie-main".analytics."products_enriched$snapshots"
ORDER BY committed_at DESC;

-- История (цепочка актуализации снапшотов)
SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM "iceberg-nessie-main".analytics."products_enriched$history"
ORDER BY made_current_at DESC;
```

### Тайм‑тревел по snapshot_id
1) Возьмите `snapshot_id` **до** следующего MERGE (например, предыдущий из `$snapshots`).
2) Выполните запрос «как было тогда»:
```sql
SELECT *
FROM "iceberg-nessie-main".analytics.products_enriched
FOR VERSION AS OF <SNAPSHOT_ID_ИЗ_СПИСКА>
ORDER BY product_id;
```

---

## 4) Обновления данных (UPDATE / DELETE / INSERT)

В Iceberg (через Nessie) оператор `MERGE` пока не поддерживается. Для обновления данных используйте комбинацию `UPDATE`, `DELETE` и `INSERT`.

### Пример: обновить цену, удалить строки и вставить новые

```sql
-- Обновим цену у продукта с id=2
UPDATE "iceberg-nessie-main".analytics.products_enriched
SET price = 850.75
WHERE product_id = 2;

-- Удалим продукты с id = 2 и 4
DELETE FROM "iceberg-nessie-main".analytics.products_enriched
WHERE product_id IN (2, 4);

-- Добавим обновлённые строки из источников
INSERT INTO "iceberg-nessie-main".analytics.products_enriched
SELECT
    c.id, c.name, c.email, co.name,
    p.id, p.product_name, p.price, p_co.name,
    c.created_at, CURRENT_TIMESTAMP
FROM postgres.public.customer c
JOIN clickhouse.default.product p   ON c.id = p.customer_id
JOIN "iceberg-jdbc".default.country co   ON c.country_id = co.id
JOIN "iceberg-jdbc".default.country p_co ON p.country_id = p_co.id
WHERE p.id IN (2, 4);
```

Таким образом можно эмулировать `MERGE`: сначала удалить старые версии строк, затем вставить обновлённые значения.
