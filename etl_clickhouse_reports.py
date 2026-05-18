import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_CONN = os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/snowflake")
CH_CONN = os.environ.get(
    "CLICKHOUSE_JDBC_URL",
    "jdbc:clickhouse://clickhouse:8123/default?compress=0",
)
PG_USER = os.environ.get("POSTGRES_USER", "user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "password")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}

CH_PROPS = {
    "user": CH_USER,
    "password": CH_PASS,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

PACKAGES = "org.postgresql:postgresql:42.7.4,com.clickhouse:clickhouse-jdbc:0.4.6"


def read_sf(spark: SparkSession, sql: str):
    return spark.read.jdbc(PG_CONN, f"({sql.strip()}) s", properties=PG_PROPS)


def write_ch(df, table: str) -> None:
    df.write.jdbc(CH_CONN, table, mode="overwrite", properties=CH_PROPS)
    print(f"wrote ClickHouse.{table}")


def main():
    spark = (
        SparkSession.builder.appName("ETL_ClickHouse_reports")
        .config("spark.jars.packages", PACKAGES)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ft = read_sf(spark, "SELECT * FROM snowflake.fact_transactions")
    it = read_sf(spark, "SELECT * FROM snowflake.dim_item").alias("i")
    bu = read_sf(spark, "SELECT * FROM snowflake.dim_buyer").alias("b")
    sh = read_sf(spark, "SELECT * FROM snowflake.dim_shop").alias("sh")
    ve = read_sf(spark, "SELECT * FROM snowflake.dim_vendor").alias("v")
    co = read_sf(spark, "SELECT * FROM snowflake.dim_country").alias("c")

    # products
    base_pi = ft.join(it, ["item_id"], "inner")
    prod_sales = (
        base_pi.groupBy("item_id", "item_name", "category")
        .agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("rating").alias("avg_rating"),
            F.avg(F.col("reviews_count").cast("double")).alias("avg_reviews"),
        )
        .withColumnRenamed("item_name", "product_name")
        .withColumnRenamed("category", "product_category")
    )
    category_revenue = base_pi.groupBy(F.col("category").alias("product_category")).agg(
        F.sum("total_amount").alias("category_revenue")
    )
    w_units = Window.orderBy(F.desc("total_units_sold"))
    report_products = (
        prod_sales.join(category_revenue, "product_category")
        .withColumn("rank_by_units", F.rank().over(w_units))
        .withColumn("is_top10", F.col("rank_by_units") <= 10)
    )
    write_ch(report_products, "report_products")

    # сustomers 
    bu_f = bu.select(
        "buyer_id",
        F.col("first_name").alias("buyer_first_name"),
        F.col("last_name").alias("buyer_last_name"),
        F.col("country_id").alias("buyer_country_id"),
    )
    buyer_ct = ft.join(bu_f, ["buyer_id"], "inner").join(
        co.alias("bc"), F.col("buyer_country_id") == F.col("bc.country_id"), "left"
    )
    cust_sales = buyer_ct.groupBy(
        "buyer_id",
        "buyer_first_name",
        "buyer_last_name",
        F.coalesce(F.col("bc.country_name"), F.lit("Unknown")).alias("buyer_country"),
    ).agg(
        F.sum("total_amount").alias("total_spent"),
        F.countDistinct("transaction_id").alias("order_count"),
        F.avg("total_amount").alias("avg_check"),
    )
    country_dist = cust_sales.groupBy("buyer_country").agg(F.count("buyer_id").alias("customers_in_country"))
    w_spend = Window.orderBy(F.desc("total_spent"))
    report_customers = (
        cust_sales.join(country_dist, "buyer_country")
        .withColumn("rank_by_spend", F.rank().over(w_spend))
        .withColumn("is_top10", F.col("rank_by_spend") <= 10)
    )
    write_ch(report_customers, "report_customers")

    # time
    ft_dates = ft.select(
        "transaction_id",
        "quantity",
        "total_amount",
        F.year("sale_date").alias("year"),
        F.month("sale_date").alias("month"),
    ).where(F.col("sale_date").isNotNull())
    time_sales = ft_dates.groupBy("year", "month").agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.countDistinct("transaction_id").alias("order_count"),
        F.avg("total_amount").alias("avg_order_size"),
        F.sum("quantity").alias("total_units"),
    )
    yearly = ft_dates.groupBy("year").agg(F.sum("total_amount").alias("yearly_revenue"))
    report_time = time_sales.join(yearly, "year")
    write_ch(report_time, "report_time")

    # stores
    sh_f = sh.select(
        "shop_id",
        F.col("shop_name").alias("store_name"),
        F.col("city").alias("store_city"),
        F.col("country_id").alias("shop_country_id"),
    )
    base_shop = ft.join(sh_f, ["shop_id"], "inner").join(
        co.alias("sc"), F.col("shop_country_id") == F.col("sc.country_id"), "left"
    )
    store_sales = (
        base_shop.groupBy(
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
    )
    city_dist_s = store_sales.groupBy("store_city").agg(F.sum("total_revenue").alias("city_revenue"))
    country_dist_shop = store_sales.groupBy("store_country").agg(
        F.sum("total_revenue").alias("country_revenue")
    )
    w_store = Window.orderBy(F.desc("total_revenue"))
    report_stores = (
        store_sales.join(city_dist_s, "store_city")
        .join(country_dist_shop, "store_country")
        .withColumn("rank_by_revenue", F.rank().over(w_store))
        .withColumn("is_top5", F.col("rank_by_revenue") <= 5)
    )
    write_ch(report_stores, "report_stores")

    # suppliers
    it_v = it.select(
        "item_id",
        "item_name",
        "category",
        "price",
        "rating",
        "reviews_count",
        F.col("vendor_id").alias("item_vendor_id"),
    )
    ve_k = ve.select(
        F.col("vendor_id").alias("sup_vendor_id"),
        F.col("vendor_name"),
        F.col("country_id").alias("sup_country_id"),
    )
    base_sv = ft.join(it_v, ["item_id"], "inner").join(
        ve_k, F.col("item_vendor_id") == F.col("sup_vendor_id"), "inner"
    )
    supplier_ct = base_sv.join(co.alias("vc"), F.col("sup_country_id") == F.col("vc.country_id"), "left")
    supp_sales = supplier_ct.groupBy(
        F.col("sup_vendor_id").alias("vendor_id"),
        F.col("vendor_name").alias("supplier_name"),
        F.coalesce(F.col("vc.country_name"), F.lit("Unknown")).alias("supplier_country"),
    ).agg(
        F.sum("total_amount").alias("total_revenue"),
        F.avg(F.col("price").cast("double")).alias("avg_product_price"),
        F.countDistinct("transaction_id").alias("order_count"),
    )
    supp_country_dist = supp_sales.groupBy("supplier_country").agg(
        F.sum("total_revenue").alias("country_revenue")
    )
    w_supp = Window.orderBy(F.desc("total_revenue"))
    report_suppliers = (
        supp_sales.join(supp_country_dist, "supplier_country")
        .withColumn("rank_by_revenue", F.rank().over(w_supp))
        .withColumn("is_top5", F.col("rank_by_revenue") <= 5)
    )
    write_ch(report_suppliers, "report_suppliers")

    # quality
    qb = ft.join(it, ["item_id"], "inner").select(
        "item_id",
        F.col("item_name").alias("product_name"),
        F.col("category").alias("product_category"),
        F.col("rating"),
        F.col("reviews_count"),
        F.col("quantity"),
        F.col("total_amount"),
    )
    qual = qb.groupBy("item_id", "product_name", "product_category").agg(
        F.avg("rating").alias("avg_rating"),
        F.avg(F.col("reviews_count").cast("double")).alias("avg_reviews"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_amount").alias("total_revenue"),
    )
    wrd = Window.orderBy(F.desc("avg_rating"))
    wra = Window.orderBy(F.asc("avg_rating"))
    wrev = Window.orderBy(F.desc("avg_reviews"))
    corr_row = qual.agg(F.corr(F.col("avg_rating"), F.col("total_units_sold")).alias("rating_vs_units_corr")).limit(1)
    report_quality = (
        qual.withColumn("rank_by_rating_high", F.rank().over(wrd))
        .withColumn("rank_by_rating_low", F.rank().over(wra))
        .withColumn("rank_by_reviews", F.rank().over(wrev))
        .crossJoin(corr_row)
    )
    write_ch(report_quality, "report_quality")

    print("All 6 ClickHouse reports created.")
    spark.stop()


if __name__ == "__main__":
    main()
