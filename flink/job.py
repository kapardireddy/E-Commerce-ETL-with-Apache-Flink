from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


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


print("=== Daily Sales ===")
table_env.sql_query("SELECT * FROM sales_per_day").execute().print()

print("=== Monthly Sales ===")
table_env.sql_query("SELECT * FROM sales_per_month").execute().print()

print("=== Sales Per Category ===")
table_env.sql_query("SELECT * FROM sales_per_category").execute().print()
