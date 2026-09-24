"""
Шаг 2 ETL-пайплайна.
Читает модель «звезда» из PostgreSQL, считает шесть аналитических витрин и
записывает их отдельными таблицами в ClickHouse (база marts).
"""

from urllib import request, parse, error
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_URL = "jdbc:postgresql://postgres:5432/spark_dwh"
PG_PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

CH_HTTP = "http://clickhouse:8123/"
CH_JDBC = "jdbc:clickhouse://clickhouse:8123/marts"
CH_USER = "user"
CH_PASSWORD = "password"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"


def build_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("clickhouse_reports")
        .config("spark.driver.extraClassPath", "/opt/spark/drivers/*")
        .config("spark.executor.extraClassPath", "/opt/spark/drivers/*")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ch(sql: str) -> None:
    """Отправляет SQL в ClickHouse по HTTP."""
    q = parse.urlencode({"user": CH_USER, "password": CH_PASSWORD})
    req = request.Request(f"{CH_HTTP}?{q}", data=sql.encode("utf-8"), method="POST")
    try:
        with request.urlopen(req, timeout=60) as resp:
            resp.read()
    except error.HTTPError as exc:
        raise RuntimeError(exc.read().decode("utf-8", "replace")) from exc


def text(name):
    return F.coalesce(F.col(name).cast("string"), F.lit(""))


def publish(df, table: str, ddl: str) -> None:
    target = f"marts.{table}"
    print(f"  {target}: {df.count()} rows")
    ch(f"DROP TABLE IF EXISTS {target}")
    ch(ddl)
    (
        df.write.mode("append").format("jdbc")
        .option("url", CH_JDBC)
        .option("dbtable", target)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", CH_DRIVER)
        .save()
    )


def main():
    spark = build_session()
    ch("CREATE DATABASE IF NOT EXISTS marts")

    fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS).select(
        "sale_id", "sale_date", "customer_id", "seller_id", "product_id",
        "store_id", "supplier_id", "pet_id",
        F.col("quantity").cast("long").alias("quantity"),
        F.col("total_price").cast("double").alias("total_price"),
    ).cache()
    # У товара есть собственное поле quantity (каталожный остаток); в отчётах оно
    # не нужно и конфликтует с quantity из факта продаж, поэтому убираем его.
    product = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS).drop("quantity")
    customer = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
    store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)
    supplier = spark.read.jdbc(PG_URL, "dim_supplier", properties=PG_PROPS)

    print("Building reports in ClickHouse:")

    # 1. Витрина продаж по продуктам.
    r_product = (
        fact.join(product, "product_id", "left")
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.count("sale_id").cast("long").alias("sales_count"),
            F.sum("quantity").cast("long").alias("total_quantity"),
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg(F.col("rating").cast("double")), 2).cast("double").alias("avg_rating"),
            F.max(F.col("reviews").cast("long")).alias("reviews"),
        )
        .select(
            F.col("product_id").cast("int"),
            text("product_name").alias("product_name"),
            text("category").alias("category"),
            text("brand").alias("brand"),
            "sales_count", "total_quantity", "revenue", "avg_rating",
            F.coalesce(F.col("reviews"), F.lit(0)).alias("reviews"),
        )
    )
    publish(r_product, "report_product_sales", """
        CREATE TABLE marts.report_product_sales (
            product_id Int32, product_name String, category String, brand String,
            sales_count Int64, total_quantity Int64, revenue Float64,
            avg_rating Float64, reviews Int64
        ) ENGINE = MergeTree() ORDER BY (total_quantity, revenue, product_id)
    """)

    # 2. Витрина продаж по клиентам.
    r_customer = (
        fact.join(customer, "customer_id", "left")
        .groupBy("customer_id", "first_name", "last_name", "email", "country")
        .agg(
            F.count("sale_id").cast("long").alias("sales_count"),
            F.sum("quantity").cast("long").alias("items_bought"),
            F.round(F.sum("total_price"), 2).cast("double").alias("total_spent"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
        )
        .select(
            F.col("customer_id").cast("int"),
            text("first_name").alias("first_name"),
            text("last_name").alias("last_name"),
            text("email").alias("email"),
            text("country").alias("country"),
            "sales_count", "items_bought", "total_spent", "avg_check",
        )
    )
    publish(r_customer, "report_customer_sales", """
        CREATE TABLE marts.report_customer_sales (
            customer_id Int32, first_name String, last_name String, email String,
            country String, sales_count Int64, items_bought Int64,
            total_spent Float64, avg_check Float64
        ) ENGINE = MergeTree() ORDER BY (total_spent, customer_id)
    """)

    # 3. Витрина продаж по времени.
    r_time = (
        fact.withColumn("year", F.year("sale_date").cast("int"))
        .withColumn("month", F.month("sale_date").cast("int"))
        .groupBy("year", "month")
        .agg(
            F.count("sale_id").cast("long").alias("sales_count"),
            F.sum("quantity").cast("long").alias("items_sold"),
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_order_value"),
        )
        .select("year", "month", "sales_count", "items_sold", "revenue", "avg_order_value")
    )
    publish(r_time, "report_time_sales", """
        CREATE TABLE marts.report_time_sales (
            year Int32, month Int32, sales_count Int64, items_sold Int64,
            revenue Float64, avg_order_value Float64
        ) ENGINE = MergeTree() ORDER BY (year, month)
    """)

    # 4. Витрина продаж по магазинам.
    r_store = (
        fact.join(store, "store_id", "left")
        .groupBy("store_id", "store_name", "city", "country")
        .agg(
            F.count("sale_id").cast("long").alias("sales_count"),
            F.sum("quantity").cast("long").alias("items_sold"),
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
        )
        .select(
            F.col("store_id").cast("int"),
            text("store_name").alias("store_name"),
            text("city").alias("city"),
            text("country").alias("country"),
            "sales_count", "items_sold", "revenue", "avg_check",
        )
    )
    publish(r_store, "report_store_sales", """
        CREATE TABLE marts.report_store_sales (
            store_id Int32, store_name String, city String, country String,
            sales_count Int64, items_sold Int64, revenue Float64, avg_check Float64
        ) ENGINE = MergeTree() ORDER BY (revenue, store_id)
    """)

    # 5. Витрина продаж по поставщикам.
    r_supplier = (
        fact.join(supplier, "supplier_id", "left")
        .join(product.select("product_id", "price"), "product_id", "left")
        .groupBy("supplier_id", "supplier_name", "country")
        .agg(
            F.count("sale_id").cast("long").alias("sales_count"),
            F.sum("quantity").cast("long").alias("items_sold"),
            F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
            F.round(F.avg(F.col("price").cast("double")), 2).cast("double").alias("avg_product_price"),
        )
        .select(
            F.col("supplier_id").cast("int"),
            text("supplier_name").alias("supplier_name"),
            text("country").alias("country"),
            "sales_count", "items_sold", "revenue", "avg_product_price",
        )
    )
    publish(r_supplier, "report_supplier_sales", """
        CREATE TABLE marts.report_supplier_sales (
            supplier_id Int32, supplier_name String, country String,
            sales_count Int64, items_sold Int64, revenue Float64, avg_product_price Float64
        ) ENGINE = MergeTree() ORDER BY (revenue, supplier_id)
    """)

    # 6. Витрина качества продукции.
    per_product = fact.groupBy("product_id").agg(
        F.count("sale_id").cast("long").alias("sales_count"),
        F.sum("quantity").cast("long").alias("items_sold"),
        F.round(F.sum("total_price"), 2).cast("double").alias("revenue"),
    )
    quality = product.join(per_product, "product_id", "left").select(
        F.col("product_id").cast("int"),
        text("product_name").alias("product_name"),
        text("category").alias("category"),
        F.col("rating").cast("double").alias("avg_rating"),
        F.col("reviews").cast("long").alias("total_reviews"),
        F.coalesce(F.col("sales_count"), F.lit(0)).cast("long").alias("sales_count"),
        F.coalesce(F.col("items_sold"), F.lit(0)).cast("long").alias("items_sold"),
        F.coalesce(F.col("revenue"), F.lit(0.0)).cast("double").alias("revenue"),
    )
    corr = quality.stat.corr("avg_rating", "items_sold")
    if corr is None or corr != corr:  # None или NaN
        corr = 0.0
    quality = quality.withColumn("rating_sales_correlation", F.lit(round(float(corr), 4)).cast("double"))
    publish(quality, "report_product_quality", """
        CREATE TABLE marts.report_product_quality (
            product_id Int32, product_name String, category String,
            avg_rating Float64, total_reviews Int64, sales_count Int64,
            items_sold Int64, revenue Float64, rating_sales_correlation Float64
        ) ENGINE = MergeTree() ORDER BY (avg_rating, product_id)
    """)

    print("All 6 ClickHouse reports are ready.")
    spark.stop()


if __name__ == "__main__":
    main()
