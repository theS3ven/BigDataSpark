CREATE DATABASE IF NOT EXISTS petstore_reports;

CREATE TABLE IF NOT EXISTS petstore_reports.report_product_sales
(
    product_id      Int32,
    product_name    String,
    category        String,
    total_quantity  Int64,
    total_revenue   Float64,
    avg_rating      Float64,
    review_count    Int32,
    revenue_rank    Int64,
    quantity_rank   Int64
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS petstore_reports.report_customer_sales
(
    customer_id     Int32,
    full_name       String,
    country         String,
    total_purchases Float64,
    avg_check       Float64,
    purchase_count  Int64,
    purchase_rank   Int64
) ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS petstore_reports.report_time_sales
(
    year           Int32,
    month          Int32,
    total_revenue  Float64,
    total_quantity Int64,
    avg_order_size Float64,
    sale_count     Int64
) ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS petstore_reports.report_store_sales
(
    store_id      Int32,
    store_name    String,
    store_city    String,
    store_country String,
    total_revenue Float64,
    avg_check     Float64,
    sale_count    Int64,
    revenue_rank  Int64
) ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS petstore_reports.report_supplier_sales
(
    supplier_id      Int32,
    supplier_name    String,
    supplier_country String,
    total_revenue    Float64,
    avg_price        Float64,
    sale_count       Int64,
    revenue_rank     Int64
) ENGINE = MergeTree()
ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS petstore_reports.report_quality
(
    product_id     Int32,
    product_name   String,
    category       String,
    rating         Float64,
    reviews        Int32,
    total_quantity Int64,
    total_revenue  Float64,
    rating_rank    Int64,
    reviews_rank   Int64
) ENGINE = MergeTree()
ORDER BY product_id;
