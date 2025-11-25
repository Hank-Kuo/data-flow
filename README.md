# Data Flow

## 🏗️ 架構概覽

```
PostgreSQL CDC 
     │
     ▼
Debezium → Kafka ─────────┐
     │       │            │
     │       ▼            ▼
     │  Schema Registry  Spark Streaming
     │                       │
     │                       ▼
     └──────────────→ Airflow (監控與排程)
                             │
                             ▼
                      Iceberg (MinIO)
                             │
                             ▼
                   Hive Metastore (metadata)
                             │
                             ▼
                          Trino
                             │
                             ▼
                         Superset
                             │
                             ▼
                             BI

Auth （Apache Ranger）：
- Kafka topic
- Hive Metastore / Iceberg tables
- Trino: SQL search engine

Optional:
- Great Expectations/Soda → Data Quality
- OpenMetadata → Data Catalog / Lineage / Glossary
- dbt → SQL Transformation model
```

## 🎯 技術堆疊

- **Spark** - 分散式計算引擎
- **Kafka + Debezium** - CDC 資料串流
- **PostgreSQL** - 業務資料庫 (啟用邏輯複製)
- **Airflow** - 工作流程管理 (可選)
- **Jupyter** - 互動式分析環境

## 📊 資料流程

```
PostgreSQL (業務資料)
   ⬇ (CDC / Debezium)
Kafka (Topics: inventory-server.*)
   ⬇
Spark Structured Streaming (ETL, mapping, filter, join)
   ⬇
即時分析 & 警報
```

## 🚀 快速開始

### 1. 啟動基礎服務

```bash
# 啟動 Kafka CDC 系統
docker compose up -d zookeeper kafka schema-registry postgres-cdc kafka-connect kafka-ui debezium-ui

# 設定 CDC 連接器
./setup-cdc.sh
```

### 2. 啟動 Spark 分析

```bash
# 啟動 Spark 和 Jupyter
docker compose up -d spark-master spark-worker jupyter-spark

# 執行 Spark streaming
./start-spark-streaming.sh
```

## 🖥️ 監控介面
| 服務 | URL | 帳號密碼 |
|------|-----|---------|
| Kafka UI | http://localhost:8086 | - |
| Debezium UI | http://localhost:8087 | - |
| Spark Master | http://localhost:8088 | - |
| Jupyter Notebook | http://localhost:8888 | - |
| Airflow (可選) | http://localhost:8081 | admin/admin |

## 🔬 測試 CDC 資料流

### 觸發資料變更

```bash
# 更新客戶資料
docker exec postgres-cdc psql -U postgres -d inventory -c "UPDATE inventory.customers SET email = 'test@example.com' WHERE id = 1;"

# 更新產品價格
docker exec postgres-cdc psql -U postgres -d inventory -c "UPDATE inventory.products SET price = 199.99 WHERE id = 1;"

# 新增訂單
docker exec postgres-cdc psql -U postgres -d inventory -c "INSERT INTO inventory.orders (customer_id, total_amount, status) VALUES (1, 299.99, 'PENDING');"
```

### 觀察 Spark 處理

Spark Streaming 會即時顯示：
- ✅ 操作統計 (插入/更新/刪除)
- 🔔 價格變更警報
- 📦 庫存變化追蹤
- 📊 即時指標統計

## 💻 Jupyter 開發

1. 訪問 http://localhost:8888
2. 開啟 `CDC_Spark_Streaming_Demo.ipynb`
3. 執行互動式 CDC 資料分析

## 📝 資料表結構

- **inventory.customers** - 客戶資料
- **inventory.products** - 產品資料
- **inventory.orders** - 訂單資料
- **inventory.order_items** - 訂單項目
- **inventory.addresses** - 地址資料

## 🛠️ 自定義開發

### Spark Streaming 應用

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Custom-CDC-App") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# 處理 CDC 資料
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "inventory-server.inventory.customers") \
    .load()
```

### 檔案結構

```
├── spark/
│   ├── streaming/              # Spark 應用程式
│   ├── data/                   # 資料目錄
│   └── logs/                   # 日誌目錄
├── notebooks/                  # Jupyter notebooks
├── dags/                       # Airflow DAGs
├── init-scripts/               # 資料庫初始化
└── setup-cdc.sh              # CDC 設定腳本
```

## 🎯 使用案例

1. **即時監控** - 監控資料庫變更，產生即時警報
2. **異常偵測** - 分析交易模式，偵測異常行為
3. **庫存管理** - 追蹤庫存變化，自動觸發補貨
4. **客戶分析** - 即時分析客戶行為變化

## 🔧 故障排除

```bash
# 檢查服務狀態
docker compose ps

# 查看日誌
docker compose logs kafka-connect
docker compose logs spark-master

# 重置系統（會清除資料）
docker compose down -v && docker compose up -d
```

---

🎉 **您的即時資料流處理平台已就緒！**

graph TD

%% Source
subgraph Source
    PG[PostgreSQL]
end

%% CDC Layer
subgraph CDC Layer
    Debezium[Debezium Connector]
    Kafka[Kafka]
    Registry[Confluent Schema Registry]
end

%% Processing Layer
subgraph Streaming & ETL
    Spark[Spark Streaming<br/>(Avro/JSON w/ Schema)]
    Checkpoint[Checkpoint / Offset Store]
end

%% Storage Layer
subgraph Lakehouse
    Iceberg[Apache Iceberg<br/>on MinIO (S3)]
    Hive[Hive Metastore]
end

%% Query/BI Layer
subgraph Query & BI
    Trino[Trino SQL Engine]
    Superset[Superset Dashboard]
end

%% Governance
subgraph Governance
    Ranger[Apache Ranger]
end

%% Modeling
subgraph Modeling
    dbt[dbt (on Trino)]
end

%% Arrows
PG --> Debezium --> Kafka
Kafka --> Registry
Kafka --> Spark
Registry --> Spark
Spark --> Iceberg
Spark --> Checkpoint
Hive --> Spark
Hive --> Trino
Iceberg --> Trino
Trino --> Superset
Trino --> dbt
Ranger --> Trino
Ranger --> Hive

Orchestration tool
- Dagster
- airflow
- argo

Others:
- OpenMetadata: Platform for data lineage(OpenLineage) and UI
- dbt: manage SQL and data lineage 
- Great Expectations: Data reconciliation
- Airbyte: CDC tool


```sql

WITH base AS (
  SELECT
    user_id,
    DATE(signup_time) AS signup_date,
    DATE(activity_time) AS activity_date
  FROM user_activity_table
),

cohorts AS (
  SELECT
    user_id,
    signup_date,
    activity_date,
    DATEDIFF(activity_date, signup_date) AS day_diff
  FROM base
  WHERE activity_date >= signup_date
),

daily_retention AS (
  SELECT
    signup_date,
    day_diff,
    COUNT(DISTINCT user_id) AS retained_users
  FROM cohorts
  GROUP BY signup_date, day_diff
),

cohort_sizes AS (
  SELECT
    signup_date,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM base
  GROUP BY signup_date
)

SELECT
  d.signup_date,
  d.day_diff,
  d.retained_users,
  c.cohort_size,
  CAST(d.retained_users AS DOUBLE) / c.cohort_size AS retention_rate
FROM daily_retention d
JOIN cohort_sizes c ON d.signup_date = c.signup_date
ORDER BY signup_date, day_diff
;
```
