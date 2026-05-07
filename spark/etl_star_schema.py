from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPS = {
    "user": "bigdata",
    "password": "bigdata123",
    "driver": "org.postgresql.Driver",
}


def write_pg(df, table):
    df.write.jdbc(url=PG_URL, table=table, mode="overwrite", properties=PG_PROPS)
    print(f"  -> записано в {table}: {df.count()} строк")


def global_rank(order_col):
    return F.dense_rank().over(Window.partitionBy(F.lit(1)).orderBy(order_col))


def main():
    spark = (
        SparkSession.builder.appName("BigDataSpark - Star Schema ETL")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("Читаем mock_data из PostgreSQL...")
    raw = spark.read.jdbc(url=PG_URL, table="mock_data", properties=PG_PROPS).cache()
    print(f"  Строк в источнике: {raw.count()}")

    # ── dim_supplier ──────────────────────────────────────────────────────────
    print("\nСтроим dim_supplier...")
    dim_supplier = (
        raw.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .distinct()
        .withColumn(
            "supplier_id", global_rank(F.struct("supplier_name", "supplier_city"))
        )
    ).cache()

    write_pg(
        dim_supplier.select(
            "supplier_id",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        ),
        "dim_supplier",
    )

    # ── dim_customer ──────────────────────────────────────────────────────────
    print("\nСтроим dim_customer...")
    dim_customer = raw.groupBy(F.col("sale_customer_id").alias("customer_id")).agg(
        F.first("customer_first_name").alias("first_name"),
        F.first("customer_last_name").alias("last_name"),
        F.first("customer_age").alias("age"),
        F.first("customer_email").alias("email"),
        F.first("customer_country").alias("country"),
        F.first("customer_postal_code").alias("postal_code"),
        F.first("customer_pet_type").alias("pet_type"),
        F.first("customer_pet_name").alias("pet_name"),
        F.first("customer_pet_breed").alias("pet_breed"),
    )
    write_pg(dim_customer, "dim_customer")

    # ── dim_seller ────────────────────────────────────────────────────────────
    print("\nСтроим dim_seller...")
    dim_seller = raw.groupBy(F.col("sale_seller_id").alias("seller_id")).agg(
        F.first("seller_first_name").alias("first_name"),
        F.first("seller_last_name").alias("last_name"),
        F.first("seller_email").alias("email"),
        F.first("seller_country").alias("country"),
        F.first("seller_postal_code").alias("postal_code"),
    )
    write_pg(dim_seller, "dim_seller")

    # ── dim_product ───────────────────────────────────────────────────────────
    print("\nСтроим dim_product...")
    product_base = raw.groupBy(F.col("sale_product_id").alias("product_id")).agg(
        F.first("product_name").alias("product_name"),
        F.first("product_category").alias("category"),
        F.first("product_price").alias("price"),
        F.first("product_quantity").alias("quantity"),
        F.first("pet_category").alias("pet_category"),
        F.first("product_weight").alias("weight"),
        F.first("product_color").alias("color"),
        F.first("product_size").alias("size"),
        F.first("product_brand").alias("brand"),
        F.first("product_material").alias("material"),
        F.first("product_description").alias("description"),
        F.first("product_rating").alias("rating"),
        F.first("product_reviews").alias("reviews"),
        F.first("product_release_date").alias("release_date_str"),
        F.first("product_expiry_date").alias("expiry_date_str"),
        F.first("supplier_name").alias("supplier_name"),
        F.first("supplier_city").alias("supplier_city"),
    )

    dim_product = product_base.join(
        dim_supplier.select("supplier_id", "supplier_name", "supplier_city"),
        on=["supplier_name", "supplier_city"],
        how="left",
    ).select(
        "product_id",
        "product_name",
        "category",
        "price",
        "quantity",
        "pet_category",
        "weight",
        "color",
        "size",
        "brand",
        "material",
        "description",
        "rating",
        "reviews",
        F.to_date(F.col("release_date_str"), "M/d/yyyy").alias("release_date"),
        F.to_date(F.col("expiry_date_str"), "M/d/yyyy").alias("expiry_date"),
        "supplier_id",
    )
    write_pg(dim_product, "dim_product")

    # ── dim_store ─────────────────────────────────────────────────────────────
    print("\nСтроим dim_store...")
    dim_store = (
        raw.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .distinct()
        .withColumn("store_id", global_rank(F.struct("store_name", "store_city")))
    ).cache()

    write_pg(
        dim_store.select(
            "store_id",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        ),
        "dim_store",
    )

    # ── dim_date ──────────────────────────────────────────────────────────────
    print("\nСтроим dim_date...")
    dim_date = (
        raw.select(F.to_date(F.col("sale_date"), "M/d/yyyy").alias("full_date"))
        .distinct()
        .filter(F.col("full_date").isNotNull())
        .withColumn("date_id", global_rank("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
    ).cache()

    write_pg(
        dim_date.select("date_id", "full_date", "day", "month", "year", "quarter"),
        "dim_date",
    )

    # ── fact_sales ────────────────────────────────────────────────────────────
    print("\nСтроим fact_sales...")
    store_lookup = dim_store.select("store_id", "store_name", "store_city").alias("sl")
    date_lookup = dim_date.select("date_id", "full_date").alias("dl")

    fact_sales = (
        raw.alias("r")
        .join(
            store_lookup,
            (F.col("r.store_name") == F.col("sl.store_name"))
            & (F.col("r.store_city") == F.col("sl.store_city")),
            how="left",
        )
        .join(
            date_lookup,
            F.to_date(F.col("r.sale_date"), "M/d/yyyy") == F.col("dl.full_date"),
            how="left",
        )
        .select(
            F.col("r.row_id").alias("sale_id"),
            F.col("r.sale_customer_id").alias("customer_id"),
            F.col("r.sale_seller_id").alias("seller_id"),
            F.col("r.sale_product_id").alias("product_id"),
            F.col("sl.store_id").alias("store_id"),
            F.col("dl.date_id").alias("date_id"),
            F.col("r.sale_quantity").alias("quantity"),
            F.col("r.sale_total_price").alias("total_price"),
        )
    )
    write_pg(fact_sales, "fact_sales")

    print("\nStar Schema ETL завершён успешно!")
    spark.stop()


if __name__ == "__main__":
    main()
