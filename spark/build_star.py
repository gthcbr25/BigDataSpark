"""
Шаг 1 ETL-пайплайна.
Читает сырую таблицу mock_data из PostgreSQL, очищает значения и строит
аналитическую модель «звезда» (шесть измерений и таблица фактов) обратно в
PostgreSQL.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

PG_URL = "jdbc:postgresql://postgres:5432/spark_dwh"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_DRIVER = "org.postgresql.Driver"
PG_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": PG_DRIVER}


def build_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("build_star_schema")
        .config("spark.driver.extraClassPath", "/opt/spark/drivers/*")
        .config("spark.executor.extraClassPath", "/opt/spark/drivers/*")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_pg_sql(spark: SparkSession, statements: str) -> None:
    """Выполняет произвольный SQL в PostgreSQL через JDBC (для view и DDL)."""
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName(PG_DRIVER)
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, PG_USER, PG_PASSWORD)
    stmt = conn.createStatement()
    try:
        for part in statements.split(";"):
            part = part.strip()
            if part:
                stmt.execute(part)
    finally:
        stmt.close()
        conn.close()


# --- функции очистки и приведения типов ---

def clean(name):
    v = F.trim(F.col(name).cast("string"))
    return F.when((v == "") | v.isNull(), F.lit(None)).otherwise(v)


def nkey(name):
    return F.coalesce(F.lower(clean(name)), F.lit("~"))


def md5_of(cols):
    return F.md5(F.concat_ws("|", *[nkey(c) for c in cols]))


def as_int(name):
    return clean(name).cast("int")


def as_money(name):
    return clean(name).cast(DecimalType(12, 2))


def as_dec(name):
    return clean(name).cast(DecimalType(12, 2))


def as_date(name):
    return F.to_date(clean(name), "M/d/yyyy")


def add_surrogate(df, key_name):
    """Присваивает измерению плотный целочисленный ключ."""
    w = Window.orderBy("natural_key")
    return df.withColumn(key_name, F.row_number().over(w))


def save(df, table):
    n = df.count()
    print(f"  {table}: {n} rows")
    df.write.mode("overwrite").option("truncate", "false").jdbc(PG_URL, table, properties=PG_PROPS)


def main():
    spark = build_session()

    run_pg_sql(spark, """
        DROP VIEW IF EXISTS view_load_quality;
        DROP VIEW IF EXISTS view_row_counts;
        DROP TABLE IF EXISTS fact_sales CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS dim_seller CASCADE;
        DROP TABLE IF EXISTS dim_product CASCADE;
        DROP TABLE IF EXISTS dim_store CASCADE;
        DROP TABLE IF EXISTS dim_supplier CASCADE;
        DROP TABLE IF EXISTS dim_pet CASCADE;
    """)

    raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS).cache()
    print(f"mock_data rows: {raw.count()}")

    # --- Измерения ---
    dim_customer = add_surrogate(
        raw.select(
            md5_of(["customer_email"]).alias("natural_key"),
            as_int("sale_customer_id").alias("src_customer_id"),
            clean("customer_first_name").alias("first_name"),
            clean("customer_last_name").alias("last_name"),
            clean("customer_email").alias("email"),
            as_int("customer_age").alias("age"),
            clean("customer_country").alias("country"),
            clean("customer_postal_code").alias("postal_code"),
        ).where(F.col("email").isNotNull()).dropDuplicates(["natural_key"]),
        "customer_id",
    )

    dim_seller = add_surrogate(
        raw.select(
            md5_of(["seller_email"]).alias("natural_key"),
            as_int("sale_seller_id").alias("src_seller_id"),
            clean("seller_first_name").alias("first_name"),
            clean("seller_last_name").alias("last_name"),
            clean("seller_email").alias("email"),
            clean("seller_country").alias("country"),
            clean("seller_postal_code").alias("postal_code"),
        ).where(F.col("email").isNotNull()).dropDuplicates(["natural_key"]),
        "seller_id",
    )

    store_cols = ["store_name", "store_location", "store_city", "store_state",
                  "store_country", "store_phone", "store_email"]
    dim_store = add_surrogate(
        raw.select(
            md5_of(store_cols).alias("natural_key"),
            clean("store_name").alias("store_name"),
            clean("store_location").alias("location"),
            clean("store_city").alias("city"),
            clean("store_state").alias("state"),
            clean("store_country").alias("country"),
            clean("store_phone").alias("phone"),
            clean("store_email").alias("email"),
        ).where(F.col("store_name").isNotNull()).dropDuplicates(["natural_key"]),
        "store_id",
    )

    supplier_cols = ["supplier_name", "supplier_contact", "supplier_email",
                     "supplier_phone", "supplier_address", "supplier_city", "supplier_country"]
    dim_supplier = add_surrogate(
        raw.select(
            md5_of(supplier_cols).alias("natural_key"),
            clean("supplier_name").alias("supplier_name"),
            clean("supplier_contact").alias("contact"),
            clean("supplier_email").alias("email"),
            clean("supplier_phone").alias("phone"),
            clean("supplier_address").alias("address"),
            clean("supplier_city").alias("city"),
            clean("supplier_country").alias("country"),
        ).where(F.col("supplier_name").isNotNull()).dropDuplicates(["natural_key"]),
        "supplier_id",
    )

    product_cols = ["product_name", "product_category", "pet_category", "product_price",
                    "product_quantity", "product_weight", "product_color", "product_size",
                    "product_brand", "product_material", "product_description",
                    "product_release_date", "product_expiry_date", "supplier_email"]
    dim_product = add_surrogate(
        raw.select(
            md5_of(product_cols).alias("natural_key"),
            as_int("sale_product_id").alias("src_product_id"),
            clean("product_name").alias("product_name"),
            clean("product_category").alias("category"),
            clean("pet_category").alias("pet_category"),
            clean("product_brand").alias("brand"),
            as_money("product_price").alias("price"),
            as_int("product_quantity").alias("quantity"),
            as_dec("product_weight").alias("weight"),
            clean("product_color").alias("color"),
            clean("product_size").alias("size"),
            clean("product_material").alias("material"),
            clean("product_description").alias("description"),
            as_dec("product_rating").alias("rating"),
            as_int("product_reviews").alias("reviews"),
            as_date("product_release_date").alias("release_date"),
            as_date("product_expiry_date").alias("expiry_date"),
        ).where(F.col("product_name").isNotNull()).dropDuplicates(["natural_key"]),
        "product_id",
    )

    pet_cols = ["customer_email", "customer_pet_name", "customer_pet_type", "customer_pet_breed"]
    dim_pet = add_surrogate(
        raw.select(
            md5_of(pet_cols).alias("natural_key"),
            clean("customer_pet_name").alias("pet_name"),
            clean("customer_pet_type").alias("pet_type"),
            clean("customer_pet_breed").alias("breed"),
        ).where(F.col("pet_name").isNotNull()).dropDuplicates(["natural_key"]),
        "pet_id",
    )

    # --- Таблица фактов ---
    fact = raw.select(
        F.col("source_row_id").cast("long").alias("source_row_id"),
        as_int("id").alias("src_sale_id"),
        as_date("sale_date").alias("sale_date"),
        md5_of(["customer_email"]).alias("k_customer"),
        md5_of(["seller_email"]).alias("k_seller"),
        md5_of(product_cols).alias("k_product"),
        md5_of(store_cols).alias("k_store"),
        md5_of(supplier_cols).alias("k_supplier"),
        md5_of(pet_cols).alias("k_pet"),
        as_int("sale_quantity").alias("quantity"),
        as_money("sale_total_price").alias("total_price"),
    )

    fact = (
        fact
        .join(dim_customer.select("customer_id", F.col("natural_key").alias("k_customer")), "k_customer", "left")
        .join(dim_seller.select("seller_id", F.col("natural_key").alias("k_seller")), "k_seller", "left")
        .join(dim_product.select("product_id", F.col("natural_key").alias("k_product")), "k_product", "left")
        .join(dim_store.select("store_id", F.col("natural_key").alias("k_store")), "k_store", "left")
        .join(dim_supplier.select("supplier_id", F.col("natural_key").alias("k_supplier")), "k_supplier", "left")
        .join(dim_pet.select("pet_id", F.col("natural_key").alias("k_pet")), "k_pet", "left")
        .withColumn(
            "unit_price",
            F.when(F.col("quantity") != 0,
                   F.round(F.col("total_price") / F.col("quantity"), 2)).cast(DecimalType(12, 2)),
        )
    )

    w = Window.orderBy("source_row_id")
    fact = fact.withColumn("sale_id", F.row_number().over(w)).select(
        "sale_id", "source_row_id", "src_sale_id", "sale_date",
        "customer_id", "seller_id", "product_id", "store_id", "supplier_id", "pet_id",
        "quantity", "total_price", "unit_price",
    )

    print("Writing star schema to PostgreSQL:")
    save(dim_customer, "dim_customer")
    save(dim_seller, "dim_seller")
    save(dim_product, "dim_product")
    save(dim_store, "dim_store")
    save(dim_supplier, "dim_supplier")
    save(dim_pet, "dim_pet")
    save(fact, "fact_sales")

    run_pg_sql(spark, """
        CREATE OR REPLACE VIEW view_row_counts AS
        SELECT 'mock_data' AS table_name, COUNT(*)::BIGINT AS rows FROM mock_data
        UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
        UNION ALL SELECT 'dim_seller',   COUNT(*) FROM dim_seller
        UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
        UNION ALL SELECT 'dim_store',    COUNT(*) FROM dim_store
        UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
        UNION ALL SELECT 'dim_pet',      COUNT(*) FROM dim_pet
        UNION ALL SELECT 'fact_sales',   COUNT(*) FROM fact_sales;

        CREATE OR REPLACE VIEW view_load_quality AS
        SELECT
            (SELECT COUNT(*) FROM mock_data)  AS source_rows,
            (SELECT COUNT(*) FROM fact_sales) AS fact_rows,
            (SELECT COUNT(*) FROM mock_data) - (SELECT COUNT(*) FROM fact_sales) AS missing_rows,
            COUNT(*) FILTER (WHERE customer_id IS NULL) AS no_customer,
            COUNT(*) FILTER (WHERE seller_id   IS NULL) AS no_seller,
            COUNT(*) FILTER (WHERE product_id  IS NULL) AS no_product,
            COUNT(*) FILTER (WHERE store_id    IS NULL) AS no_store,
            COUNT(*) FILTER (WHERE supplier_id IS NULL) AS no_supplier,
            COUNT(*) FILTER (WHERE pet_id      IS NULL) AS no_pet
        FROM fact_sales;
    """)

    print("Star schema is ready.")
    spark.stop()


if __name__ == "__main__":
    main()
