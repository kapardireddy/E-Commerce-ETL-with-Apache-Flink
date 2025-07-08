from pyflink.table import EnvironmentSettings, TableEnvironment

#Flink streaming environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Kafka Source Table
table_env.execute_sql("""
CREATE TABLE transactions (
    transactionId STRING,
    productId STRING,
    productName STRING,
    productCategory STRING,
    productPrice DOUBLE,
    productQuantity INT,
    productBrand STRING,
    currency STRING,
    customerId STRING,
    transactionDate TIMESTAMP(3),
    paymentMethod STRING,
    WATERMARK FOR transactionDate AS transactionDate - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# PostgreSQL Sink Tables

# sales_per_day
table_env.execute_sql("""
CREATE TABLE sales_per_day_pg (
    sale_day STRING,
    total_sales DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/flinkuser',
    'table-name' = 'sales_per_day',
    'username' = 'flinkuser',
    'password' = 'flinkpw'
)
""")

# sales_per_month
table_env.execute_sql("""
CREATE TABLE sales_per_month_pg (
    sale_month STRING,
    total_sales DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/flinkuser',
    'table-name' = 'sales_per_month',
    'username' = 'flinkuser',
    'password' = 'flinkpw'
)
""")

# sales_per_category
table_env.execute_sql("""
CREATE TABLE sales_per_category_pg (
    productCategory STRING,
    total_sales DOUBLE
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/flinkuser',
    'table-name' = 'sales_per_category',
    'username' = 'flinkuser',
    'password' = 'flinkpw'
)
""")

# Elasticsearch Sink Table
table_env.execute_sql("""
CREATE TABLE transactions_es (
    transactionId STRING,
    productId STRING,
    productName STRING,
    productCategory STRING,
    productPrice DOUBLE,
    productQuantity INT,
    productBrand STRING,
    currency STRING,
    customerId STRING,
    transactionDate TIMESTAMP(3),
    paymentMethod STRING
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'transactions',
    'document-id.key-delimiter' = '$',
    'document-id.key.fields' = 'transactionId',
    'format' = 'json'
)
""")


# Aggregated Views

table_env.execute_sql("""
CREATE TEMPORARY VIEW sales_per_day AS
SELECT
    DATE_FORMAT(transactionDate, 'yyyy-MM-dd') AS sale_day,
    SUM(productPrice * productQuantity) AS total_sales
FROM transactions
GROUP BY DATE_FORMAT(transactionDate, 'yyyy-MM-dd')
""")

table_env.execute_sql("""
CREATE TEMPORARY VIEW sales_per_month AS
SELECT
    DATE_FORMAT(transactionDate, 'yyyy-MM') AS sale_month,
    SUM(productPrice * productQuantity) AS total_sales
FROM transactions
GROUP BY DATE_FORMAT(transactionDate, 'yyyy-MM')
""")

table_env.execute_sql("""
CREATE TEMPORARY VIEW sales_per_category AS
SELECT
    productCategory,
    SUM(productPrice * productQuantity) AS total_sales
FROM transactions
GROUP BY productCategory
""")

# Insert into PostgreSQL
table_env.execute_sql("INSERT INTO sales_per_day_pg SELECT * FROM sales_per_day")
table_env.execute_sql("INSERT INTO sales_per_month_pg SELECT * FROM sales_per_month")
table_env.execute_sql("INSERT INTO sales_per_category_pg SELECT * FROM sales_per_category")

# Insert into Elasticsearch

table_env.execute_sql("""
INSERT INTO transactions_es
SELECT
    transactionId,
    productId,
    productName,
    productCategory,
    productPrice,
    productQuantity,
    productBrand,
    currency,
    customerId,
    transactionDate,
    paymentMethod
FROM transactions
""")
