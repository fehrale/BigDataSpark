import os
from urllib.parse import quote

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_CONN = os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/snowflake")
PG_USER = os.environ.get("POSTGRES_USER", "user")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "password")
PG_PACKAGES = "org.postgresql:postgresql:42.7.4"

PG_PROPS = {
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver",
}


def jdbc_exec(spark: SparkSession, sql: str) -> None:
    jvm = spark._jvm  # noqa: SLF001
    loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    driver_class = jvm.java.lang.Class.forName("org.postgresql.Driver", True, loader)
    driver = driver_class.newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", PG_USER)
    props.setProperty("password", PG_PASS)
    conn = driver.connect(PG_CONN, props)
    if conn is None:
        raise RuntimeError(f"JDBC driver rejected URL: {PG_CONN}")
    try:
        st = conn.createStatement()
        try:
            st.execute(sql)
        finally:
            st.close()
    finally:
        conn.close()


def read_mock(spark: SparkSession):
    return spark.read.jdbc(PG_CONN, "(SELECT * FROM public.mock_data) m", properties=PG_PROPS)


def write_snowflake_table(df, table_name: str) -> None:
    base = PG_CONN.split("?", 1)[0]
    write_url = base + "?options=" + quote("-c search_path=snowflake", safe="")
    df.write.jdbc(write_url, table_name, mode="append", properties=PG_PROPS)




def main():
    spark = (
        SparkSession.builder.appName("ETL_star_schema")
        .config("spark.jars.packages", PG_PACKAGES)
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    jdbc_exec(
        spark,
        """
        TRUNCATE TABLE
          snowflake.fact_transactions,
          snowflake.dim_item,
          snowflake.dim_vendor,
          snowflake.dim_shop,
          snowflake.dim_employee,
          snowflake.dim_animal,
          snowflake.dim_buyer,
          snowflake.dim_country
        RESTART IDENTITY CASCADE
        """.strip(),
    )

    m = read_mock(spark)

    cust_c = (
        m.select(F.col("customer_country").alias("country_name")).where(
            F.col("country_name").isNotNull() & (F.trim(F.col("country_name")) != "")
        )
    )
    sell_c = (
        m.select(F.col("seller_country").alias("country_name")).where(
            F.col("country_name").isNotNull() & (F.trim(F.col("country_name")) != "")
        )
    )
    store_c = (
        m.select(F.col("store_country").alias("country_name")).where(
            F.col("country_name").isNotNull() & (F.trim(F.col("country_name")) != "")
        )
    )
    supp_c = (
        m.select(F.col("supplier_country").alias("country_name")).where(
            F.col("country_name").isNotNull() & (F.trim(F.col("country_name")) != "")
        )
    )
    country_names = cust_c.unionByName(sell_c).unionByName(store_c).unionByName(supp_c).distinct()
    write_snowflake_table(country_names, "dim_country")

    dim_country = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_country) c", properties=PG_PROPS)

    dim_buyer = (
        m.join(dim_country, m.customer_country == F.col("country_name"), "left")
        .where(F.col("customer_first_name").isNotNull())
        .select(
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("country_id"),
            F.col("customer_postal_code").alias("postal_code"),
        )
        .distinct()
    )
    write_snowflake_table(dim_buyer, "dim_buyer")

    dim_animal = (
        m.where(F.col("customer_pet_name").isNotNull())
        .select(
            F.col("customer_pet_name").alias("animal_name"),
            F.col("customer_pet_type").alias("animal_type"),
            F.col("customer_pet_breed").alias("animal_breed"),
            F.col("pet_category").alias("animal_category"),
        )
        .dropDuplicates(["animal_name", "animal_type"])
    )
    write_snowflake_table(dim_animal, "dim_animal")

    dim_employee = (
        m.join(dim_country.alias("wc"), m.seller_country == F.col("wc.country_name"), "left")
        .where(F.col("seller_first_name").isNotNull())
        .select(
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("wc.country_id"),
            F.col("seller_postal_code").alias("postal_code"),
        )
        .distinct()
    )
    write_snowflake_table(dim_employee, "dim_employee")

    dim_shop = (
        m.join(dim_country.alias("sc"), m.store_country == F.col("sc.country_name"), "left")
        .where(F.col("store_name").isNotNull())
        .select(
            F.col("store_name").alias("shop_name"),
            F.col("store_location").alias("address"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("region"),
            F.col("sc.country_id"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .dropDuplicates(["shop_name", "city"])
    )
    write_snowflake_table(dim_shop, "dim_shop")

    dim_vendor = (
        m.join(dim_country.alias("vc"), m.supplier_country == F.col("vc.country_name"), "left")
        .where(F.trim(F.coalesce(F.col("supplier_name"), F.lit(""))) != "")
        .select(
            F.col("supplier_name").alias("vendor_name"),
            F.col("supplier_contact").alias("contact_person"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("vc.country_id"),
        )
        .dropDuplicates(["vendor_name"])
    )
    write_snowflake_table(dim_vendor, "dim_vendor")

    dim_vendor_r = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_vendor) v", properties=PG_PROPS)
    dim_item = (
        m.join(dim_vendor_r, m.supplier_name == F.col("vendor_name"), "left")
        .where(F.col("product_name").isNotNull())
        .select(
            F.col("product_name").alias("item_name"),
            F.col("product_category").alias("category"),
            F.col("product_price").cast("decimal(10,2)").alias("price"),
            F.col("product_weight").cast("decimal(10,2)").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").cast("decimal(3,2)").alias("rating"),
            F.col("product_reviews").cast("int").alias("reviews_count"),
            (
                F.when(
                    F.trim(F.coalesce(F.col("product_release_date"), F.lit(""))) != "",
                    F.to_date(F.trim(F.col("product_release_date")), "M/d/yyyy"),
                )
            ).alias("release_date"),
            (
                F.when(
                    F.trim(F.coalesce(F.col("product_expiry_date"), F.lit(""))) != "",
                    F.to_date(F.trim(F.col("product_expiry_date")), "M/d/yyyy"),
                )
            ).alias("expiry_date"),
            F.col("vendor_id"),
        )
        .dropDuplicates(["item_name", "category", "vendor_id"])
    )
    write_snowflake_table(dim_item, "dim_item")

    buyers = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_buyer) b", properties=PG_PROPS).alias(
        "b_dim"
    )
    animals = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_animal) a", properties=PG_PROPS).alias(
        "a_dim"
    )
    employees = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_employee) e", properties=PG_PROPS).alias(
        "e_dim"
    )
    shops = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_shop) s", properties=PG_PROPS).alias("s_dim")
    items = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_item) i", properties=PG_PROPS).alias("i_dim")
    v_dim = spark.read.jdbc(PG_CONN, "(SELECT * FROM snowflake.dim_vendor) vv", properties=PG_PROPS).alias("v_dim")

    fact = (
        m.join(
            buyers,
            (m.customer_first_name == F.col("b_dim.first_name"))
            & (m.customer_last_name == F.col("b_dim.last_name"))
            & (m.customer_email == F.col("b_dim.email")),
            "left",
        )
        .join(
            animals,
            (m.customer_pet_name == F.col("a_dim.animal_name"))
            & (m.customer_pet_type == F.col("a_dim.animal_type")),
            "left",
        )
        .join(
            employees,
            (m.seller_first_name == F.col("e_dim.first_name"))
            & (m.seller_last_name == F.col("e_dim.last_name"))
            & (m.seller_email == F.col("e_dim.email")),
            "left",
        )
        .join(
            shops,
            (m.store_name == F.col("s_dim.shop_name")) & (m.store_city == F.col("s_dim.city")),
            "left",
        )
        .join(v_dim, m.supplier_name == F.col("v_dim.vendor_name"), "left")
        .join(
            items,
            (m.product_name == F.col("i_dim.item_name"))
            & (m.product_category == F.col("i_dim.category"))
            & F.col("i_dim.vendor_id").eqNullSafe(F.col("v_dim.vendor_id")),
            "left",
        )
        .where(F.col("sale_quantity").isNotNull())
        .select(
            F.col("b_dim.buyer_id").alias("buyer_id"),
            F.col("a_dim.animal_id").alias("animal_id"),
            F.col("e_dim.employee_id").alias("employee_id"),
            F.col("s_dim.shop_id").alias("shop_id"),
            F.col("i_dim.item_id").alias("item_id"),
            F.to_timestamp(F.trim(F.coalesce(F.col("sale_date"), F.lit(""))), "M/d/yyyy").alias("sale_date"),
            F.col("sale_quantity").cast("int").alias("quantity"),
            F.col("sale_total_price").cast("decimal(10,2)").alias("total_amount"),
        )
    )
    write_snowflake_table(fact, "fact_transactions")

    print("Star schema load complete.")
    spark.stop()


if __name__ == "__main__":
    main()
