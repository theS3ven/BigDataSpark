import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPS = {
    "user": "bigdata",
    "password": "bigdata123",
    "driver": "org.postgresql.Driver",
}

CH_HOST = "http://clickhouse:8123"
CH_DB = "petstore_reports"
CH_AUTH = "?user=bigdata&password=bigdata123"

CH_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/petstore_reports"
CH_JDBC_PROPS = {
    "user": "bigdata",
    "password": "bigdata123",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
}

# Схемы таблиц: (ddl_columns, order_by)
TABLE_DDL = {
    "report_product_sales": (
        "product_id Int32, product_name String, category String, "
        "total_quantity Int64, total_revenue Float64, avg_rating Float64, "
        "review_count Int32, revenue_rank Int64, quantity_rank Int64",
        "product_id",
    ),
    "report_customer_sales": (
        "customer_id Int32, full_name String, country String, "
        "total_purchases Float64, avg_check Float64, "
        "purchase_count Int64, purchase_rank Int64",
        "customer_id",
    ),
    "report_time_sales": (
        "year Int32, month Int32, total_revenue Float64, "
        "total_quantity Int64, avg_order_size Float64, sale_count Int64",
        "year, month",
    ),
    "report_store_sales": (
        "store_id Int32, store_name String, store_city String, store_country String, "
        "total_revenue Float64, avg_check Float64, sale_count Int64, revenue_rank Int64",
        "store_id",
    ),
    "report_supplier_sales": (
        "supplier_id Int32, supplier_name String, supplier_country String, "
        "total_revenue Float64, avg_price Float64, sale_count Int64, revenue_rank Int64",
        "supplier_id",
    ),
    "report_quality": (
        "product_id Int32, product_name String, category String, "
        "rating Float64, reviews Int32, total_quantity Int64, "
        "total_revenue Float64, rating_rank Int64, reviews_rank Int64",
        "product_id",
    ),
}


# ── ClickHouse HTTP (только DDL) ───────────────────────────────────────────────


def ch_execute(sql):
    url = CH_HOST + "/" + CH_AUTH
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return r.read()
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse {e.code} на запросе «{sql[:120]}»:\n{msg}"
        ) from None


# ── Spark helpers ──────────────────────────────────────────────────────────────


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPS)


def desc_rank(col):
    return F.dense_rank().over(Window.partitionBy(F.lit(1)).orderBy(F.col(col).desc()))


def write_ch(df, table):
    """DROP + CREATE (MergeTree) + Spark JDBC bulk-insert в ClickHouse."""
    cols_ddl, order_by = TABLE_DDL[table]
    ch_execute(f"DROP TABLE IF EXISTS {CH_DB}.{table}")
    ch_execute(
        f"CREATE TABLE {CH_DB}.{table} ({cols_ddl}) "
        f"ENGINE = MergeTree() ORDER BY ({order_by})"
    )

    count = df.count()
    df.write.option("isolationLevel", "NONE").jdbc(
        url=CH_JDBC_URL,
        table=f"{CH_DB}.{table}",
        mode="append",
        properties=CH_JDBC_PROPS,
    )
    print(f"  -> записано в {table}: {count} строк")


# ── Main ───────────────────────────────────────────────────────────────────────


def main():
    spark = (
        SparkSession.builder.appName("BigDataSpark - ClickHouse Reports ETL")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("Инициализируем базу данных ClickHouse...")
    ch_execute(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")

    print("Читаем схему-звезду из PostgreSQL...")
    fact = read_pg(spark, "fact_sales").cache()
    dim_product = read_pg(spark, "dim_product").cache()
    dim_customer = read_pg(spark, "dim_customer")
    dim_store = read_pg(spark, "dim_store")
    dim_supplier = read_pg(spark, "dim_supplier")
    dim_date = read_pg(spark, "dim_date")

    fact = fact.withColumn("total_price", F.col("total_price").cast("double"))
    dim_product = dim_product.withColumn("price", F.col("price").cast("double"))

    dp_sales = dim_product.select(
        "product_id", "product_name", "category", "rating", "reviews"
    )
    dp_supply = dim_product.select("product_id", "supplier_id", "price")
    dp_quality = dim_product.select(
        "product_id", "product_name", "category", "rating", "reviews"
    )

    # ── 1. Витрина продаж по продуктам ────────────────────────────────────────
    print("\n[1/6] report_product_sales...")
    report1 = (
        fact.join(dp_sales, "product_id")
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_price").alias("total_revenue"),
            F.first("rating").cast("double").alias("avg_rating"),
            F.first("reviews").cast("int").alias("review_count"),
        )
        .withColumn("revenue_rank", desc_rank("total_revenue"))
        .withColumn("quantity_rank", desc_rank("total_quantity"))
        .select(
            F.col("product_id").cast("int"),
            "product_name",
            "category",
            F.col("total_quantity").cast("long"),
            F.col("total_revenue").cast("double"),
            F.col("avg_rating").cast("double"),
            F.col("review_count").cast("int"),
            F.col("revenue_rank").cast("long"),
            F.col("quantity_rank").cast("long"),
        )
    )
    write_ch(report1, "report_product_sales")

    # ── 2. Витрина продаж по клиентам ─────────────────────────────────────────
    print("\n[2/6] report_customer_sales...")
    report2 = (
        fact.join(dim_customer, "customer_id")
        .groupBy("customer_id", "first_name", "last_name", "country")
        .agg(
            F.sum("total_price").alias("total_purchases"),
            F.avg("total_price").alias("avg_check"),
            F.count("sale_id").alias("purchase_count"),
        )
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("purchase_rank", desc_rank("total_purchases"))
        .select(
            F.col("customer_id").cast("int"),
            "full_name",
            "country",
            F.col("total_purchases").cast("double"),
            F.col("avg_check").cast("double"),
            F.col("purchase_count").cast("long"),
            F.col("purchase_rank").cast("long"),
        )
    )
    write_ch(report2, "report_customer_sales")

    # ── 3. Витрина продаж по времени ──────────────────────────────────────────
    print("\n[3/6] report_time_sales...")
    report3 = (
        fact.join(dim_date, "date_id")
        .groupBy("year", "month")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
            F.avg("total_price").alias("avg_order_size"),
            F.count("sale_id").alias("sale_count"),
        )
        .select(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            F.col("total_revenue").cast("double"),
            F.col("total_quantity").cast("long"),
            F.col("avg_order_size").cast("double"),
            F.col("sale_count").cast("long"),
        )
        .orderBy("year", "month")
    )
    write_ch(report3, "report_time_sales")

    # ── 4. Витрина продаж по магазинам ────────────────────────────────────────
    print("\n[4/6] report_store_sales...")
    report4 = (
        fact.join(dim_store, "store_id")
        .groupBy("store_id", "store_name", "store_city", "store_country")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.avg("total_price").alias("avg_check"),
            F.count("sale_id").alias("sale_count"),
        )
        .withColumn("revenue_rank", desc_rank("total_revenue"))
        .select(
            F.col("store_id").cast("int"),
            "store_name",
            "store_city",
            "store_country",
            F.col("total_revenue").cast("double"),
            F.col("avg_check").cast("double"),
            F.col("sale_count").cast("long"),
            F.col("revenue_rank").cast("long"),
        )
    )
    write_ch(report4, "report_store_sales")

    # ── 5. Витрина продаж по поставщикам ──────────────────────────────────────
    print("\n[5/6] report_supplier_sales...")
    report5 = (
        fact.join(dp_supply, "product_id")
        .join(dim_supplier, "supplier_id")
        .groupBy("supplier_id", "supplier_name", "supplier_country")
        .agg(
            F.sum("total_price").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.count("sale_id").alias("sale_count"),
        )
        .withColumn("revenue_rank", desc_rank("total_revenue"))
        .select(
            F.col("supplier_id").cast("int"),
            "supplier_name",
            "supplier_country",
            F.col("total_revenue").cast("double"),
            F.col("avg_price").cast("double"),
            F.col("sale_count").cast("long"),
            F.col("revenue_rank").cast("long"),
        )
    )
    write_ch(report5, "report_supplier_sales")

    # ── 6. Витрина качества продукции ─────────────────────────────────────────
    print("\n[6/6] report_quality...")
    report6 = (
        fact.join(dp_quality, "product_id")
        .groupBy("product_id", "product_name", "category", "rating", "reviews")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_price").alias("total_revenue"),
        )
        .withColumn("rating_rank", desc_rank("rating"))
        .withColumn("reviews_rank", desc_rank("reviews"))
        .select(
            F.col("product_id").cast("int"),
            "product_name",
            "category",
            F.col("rating").cast("double"),
            F.col("reviews").cast("int"),
            F.col("total_quantity").cast("long"),
            F.col("total_revenue").cast("double"),
            F.col("rating_rank").cast("long"),
            F.col("reviews_rank").cast("long"),
        )
    )
    write_ch(report6, "report_quality")

    print("\nClickHouse Reports ETL завершён успешно!")
    spark.stop()


if __name__ == "__main__":
    main()
