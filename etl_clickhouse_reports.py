"""
Spark ETL: snowflake star schema (PostgreSQL) -> 18 ClickHouse tables.
6 отчётов × 3 таблицы = 18. Каждая пуля README — отдельная таблица.
Top-N через .orderBy().limit() — без Window без partitionBy.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_CONN = os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/snowflake")
CH_CONN = os.environ.get(
    "CLICKHOUSE_JDBC_URL",
    "jdbc:clickhouse://clickhouse:8123/default?compress=0",
)
PG_USER = os.environ.get("POSTGRES_USER", "user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "password")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")

PG_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}
CH_PROPS = {"user": CH_USER, "password": CH_PASS, "driver": "com.clickhouse.jdbc.ClickHouseDriver"}
PACKAGES = "org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.4.6"


def read_sf(spark: SparkSession, sql: str):
    return spark.read.jdbc(PG_CONN, f"({sql.strip()}) s", properties=PG_PROPS)


def write_ch(df, table: str) -> None:
    df.write.jdbc(CH_CONN, table, mode="overwrite", properties=CH_PROPS)
    print(f"  wrote {table}")


def main():
    spark = (
        SparkSession.builder.appName("ETL_ClickHouse_reports")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ft = read_sf(spark, "SELECT * FROM snowflake.fact_transactions").cache()
    it = read_sf(spark, "SELECT * FROM snowflake.dim_item").alias("i")
    bu = read_sf(spark, "SELECT * FROM snowflake.dim_buyer").alias("b")
    sh = read_sf(spark, "SELECT * FROM snowflake.dim_shop").alias("sh")
    ve = read_sf(spark, "SELECT * FROM snowflake.dim_vendor").alias("v")
    co = read_sf(spark, "SELECT * FROM snowflake.dim_country").alias("c")

    # ── 1. PRODUCTS ───────────────────────────────────────────────────────────
    print("1. Products")
    base_pi = ft.join(it, ["item_id"], "inner").cache()

    prod_agg = (
        base_pi.groupBy("item_id", "item_name", "category")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumnRenamed("item_name", "product_name")
        .withColumnRenamed("category", "product_category")
        .cache()
    )
    # топ-10 самых продаваемых
    write_ch(prod_agg.orderBy(F.desc("total_units_sold")).limit(10), "report_products_top10")
    # выручка по категориям
    write_ch(
        base_pi.groupBy(F.col("category").alias("product_category")).agg(
            F.sum("total_amount").alias("category_revenue"),
            F.sum("quantity").alias("total_units_sold"),
        ),
        "report_products_by_category",
    )
    # рейтинг и отзывы по каждому продукту
    write_ch(
        base_pi.groupBy("item_id", "item_name", "category")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.avg(F.col("reviews_count").cast("double")).alias("avg_reviews"),
        )
        .withColumnRenamed("item_name", "product_name")
        .withColumnRenamed("category", "product_category"),
        "report_products_ratings",
    )
    base_pi.unpersist()
    prod_agg.unpersist()

    # ── 2. CUSTOMERS ──────────────────────────────────────────────────────────
    print("2. Customers")
    bu_f = bu.select(
        "buyer_id",
        F.col("first_name").alias("buyer_first_name"),
        F.col("last_name").alias("buyer_last_name"),
        F.col("country_id").alias("buyer_country_id"),
    )
    cust_agg = (
        ft.join(bu_f, ["buyer_id"], "inner")
        .join(co.alias("bc"), F.col("buyer_country_id") == F.col("bc.country_id"), "left")
        .groupBy(
            "buyer_id",
            "buyer_first_name",
            "buyer_last_name",
            F.coalesce(F.col("bc.country_name"), F.lit("Unknown")).alias("buyer_country"),
        )
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.countDistinct("transaction_id").alias("order_count"),
            F.avg("total_amount").alias("avg_check"),
        )
        .cache()
    )
    # топ-10 по сумме покупок
    write_ch(cust_agg.orderBy(F.desc("total_spent")).limit(10), "report_customers_top10")
    # распределение по странам
    write_ch(
        cust_agg.groupBy("buyer_country").agg(
            F.count("buyer_id").alias("customer_count"),
            F.sum("total_spent").alias("country_total_spent"),
        ),
        "report_customers_by_country",
    )
    # средний чек для каждого клиента
    write_ch(
        cust_agg.select("buyer_id", "buyer_first_name", "buyer_last_name", "buyer_country", "avg_check", "order_count"),
        "report_customers_avg_check",
    )
    cust_agg.unpersist()

    # ── 3. TIME ───────────────────────────────────────────────────────────────
    print("3. Time")
    ft_dates = (
        ft.select(
            "transaction_id", "quantity", "total_amount",
            F.year("sale_date").alias("year"),
            F.month("sale_date").alias("month"),
            F.dayofweek("sale_date").alias("weekday"),
        )
        .where(F.col("sale_date").isNotNull())
        .cache()
    )
    # месячные тренды
    write_ch(
        ft_dates.groupBy("year", "month").agg(
            F.sum("total_amount").alias("monthly_revenue"),
            F.countDistinct("transaction_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_size"),
            F.sum("quantity").alias("total_units"),
        ),
        "report_time_monthly",
    )
    # годовые тренды (сравнение периодов)
    write_ch(
        ft_dates.groupBy("year").agg(
            F.sum("total_amount").alias("yearly_revenue"),
            F.countDistinct("transaction_id").alias("yearly_orders"),
            F.sum("quantity").alias("yearly_units"),
        ),
        "report_time_yearly",
    )
    # средний размер заказа по дням недели
    write_ch(
        ft_dates.groupBy("weekday").agg(
            F.avg("total_amount").alias("avg_order_size"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("transaction_id").alias("order_count"),
        ),
        "report_time_weekday",
    )
    ft_dates.unpersist()

    # ── 4. STORES ─────────────────────────────────────────────────────────────
    print("4. Stores")
    sh_f = sh.select(
        "shop_id",
        F.col("shop_name").alias("store_name"),
        F.col("city").alias("store_city"),
        F.col("country_id").alias("shop_country_id"),
    )
    store_agg = (
        ft.join(sh_f, ["shop_id"], "inner")
        .join(co.alias("sc"), F.col("shop_country_id") == F.col("sc.country_id"), "left")
        .groupBy(
            "shop_id",
            "store_name",
            "store_city",
            F.coalesce(F.col("sc.country_name"), F.lit("Unknown")).alias("store_country"),
        )
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("transaction_id").alias("order_count"),
            F.avg("total_amount").alias("avg_check"),
        )
        .cache()
    )
    # топ-5 по выручке
    write_ch(store_agg.orderBy(F.desc("total_revenue")).limit(5), "report_stores_top5")
    # распределение по городам
    write_ch(
        store_agg.groupBy("store_city").agg(
            F.sum("total_revenue").alias("city_revenue"),
            F.countDistinct("shop_id").alias("store_count"),
            F.sum("order_count").alias("total_orders"),
        ),
        "report_stores_by_city",
    )
    # распределение по странам
    write_ch(
        store_agg.groupBy("store_country").agg(
            F.sum("total_revenue").alias("country_revenue"),
            F.countDistinct("shop_id").alias("store_count"),
        ),
        "report_stores_by_country",
    )
    store_agg.unpersist()

    # ── 5. SUPPLIERS ──────────────────────────────────────────────────────────
    print("5. Suppliers")
    it_v = it.select("item_id", "item_name", "category", "price", "vendor_id")
    ve_k = ve.select(
        F.col("vendor_id").alias("sup_vendor_id"),
        F.col("vendor_name"),
        F.col("country_id").alias("sup_country_id"),
    )
    supp_agg = (
        ft.join(it_v, ["item_id"], "inner")
        .join(ve_k, F.col("vendor_id") == F.col("sup_vendor_id"), "inner")
        .join(co.alias("vc"), F.col("sup_country_id") == F.col("vc.country_id"), "left")
        .groupBy(
            F.col("sup_vendor_id").alias("vendor_id"),
            F.col("vendor_name").alias("supplier_name"),
            F.coalesce(F.col("vc.country_name"), F.lit("Unknown")).alias("supplier_country"),
        )
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.avg(F.col("price").cast("double")).alias("avg_product_price"),
            F.countDistinct("transaction_id").alias("order_count"),
        )
        .cache()
    )
    # топ-5 по выручке
    write_ch(supp_agg.orderBy(F.desc("total_revenue")).limit(5), "report_suppliers_top5")
    # средняя цена товаров по поставщику
    write_ch(
        supp_agg.select("vendor_id", "supplier_name", "supplier_country", "avg_product_price", "order_count"),
        "report_suppliers_avg_price",
    )
    # распределение по странам поставщиков
    write_ch(
        supp_agg.groupBy("supplier_country").agg(
            F.sum("total_revenue").alias("total_revenue"),
            F.countDistinct("vendor_id").alias("supplier_count"),
        ),
        "report_suppliers_by_country",
    )
    supp_agg.unpersist()

    # ── 6. QUALITY ────────────────────────────────────────────────────────────
    print("6. Quality")
    qual_agg = (
        ft.join(it, ["item_id"], "inner")
        .groupBy("item_id", "item_name", "category")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.avg(F.col("reviews_count").cast("double")).alias("avg_reviews"),
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
        )
        .withColumnRenamed("item_name", "product_name")
        .withColumnRenamed("category", "product_category")
        .cache()
    )
    # наивысший рейтинг
    write_ch(qual_agg.orderBy(F.desc("avg_rating")).limit(10), "report_quality_top_rated")
    # наименьший рейтинг
    write_ch(qual_agg.orderBy(F.asc("avg_rating")).limit(10), "report_quality_low_rated")
    # наибольшее число отзывов
    write_ch(qual_agg.orderBy(F.desc("avg_reviews")).limit(10), "report_quality_most_reviewed")
    qual_agg.unpersist()

    ft.unpersist()
    print("All 18 ClickHouse tables created.")
    spark.stop()


if __name__ == "__main__":
    main()
