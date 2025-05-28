from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, asc, desc, year, month, quarter, lag, countDistinct, corr
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from time import sleep


spark = SparkSession.builder \
        .appName("Spark SQL with PostgreSQL and ClickHouse") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()
    
pg_jdbc_url = "jdbc:postgresql://localhost:5432/spark_db"
postgres_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

ch_jdbc_url = "jdbc:clickhouse://localhost:8123/default"
clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "custom_user",
    "password": "custom_password"
}

dates = spark.read.jdbc(url=pg_jdbc_url, table="d_dates", properties=postgres_properties)
cities = spark.read.jdbc(url=pg_jdbc_url, table="d_cities", properties=postgres_properties)
countries = spark.read.jdbc(url=pg_jdbc_url, table="d_countries", properties=postgres_properties)
product_categories = spark.read.jdbc(url=pg_jdbc_url, table="d_product_categories", properties=postgres_properties)
products = spark.read.jdbc(url=pg_jdbc_url, table="d_products", properties=postgres_properties)
sales = spark.read.jdbc(url=pg_jdbc_url, table="f_sales", properties=postgres_properties)
customers = spark.read.jdbc(url=pg_jdbc_url, table="d_customers", properties=postgres_properties)
stores = spark.read.jdbc(url=pg_jdbc_url, table="d_stores", properties=postgres_properties)
suppliers = spark.read.jdbc(url=pg_jdbc_url, table="d_suppliers", properties=postgres_properties)

def DataMartProducts():
    products_top = sales.groupBy("product_id") \
        .agg(sum("product_quantity").alias("total_quantity_sold")) \
        .orderBy(desc("total_quantity_sold")) \
        .limit(10) \
        .join(
            products.alias("products"),
            col("product_id") == products.id,
            "left"
        ).select(
            col("product_id"),
            col("products.name").alias("product_name"),
            col("total_quantity_sold")
        )
    products_top.write.jdbc(url=ch_jdbc_url, table="products_top", mode="append", properties=clickhouse_properties)

    products_total_revenue = sales.join(
        products.alias("products"), 
        sales.product_id == products.id, 
        "inner"
    ) \
    .join(
        product_categories.alias("product_categories"),
        products.category_id == product_categories.id
    ) \
    .select(
        col("products.category_id"),
        col("product_categories.name"),
        col("total_price")
    ) \
    .groupBy("category_id") \
    .agg(sum("total_price").alias("total_revenue")) \
    .orderBy(desc("total_revenue"))
    products_total_revenue.write.jdbc(url=ch_jdbc_url, table="products_total_revenue", mode="append", properties=clickhouse_properties)


def DataMartCustomers():
    customers_top = sales.groupBy("customer_id") \
        .agg(sum("total_price").alias("total_price")) \
        .orderBy(desc("total_price")) \
        .limit(10) \
        .join(
            customers.alias("customers"),
            col("customer_id") == customers.id,
            "left"
        ).select(
            col("customer_id"),
            col("customers.first_name").alias("first_name"),
            col("customers.last_name").alias("last_name"),
            col("total_price")
        )
    customers_top.write.jdbc(url=ch_jdbc_url, table="customers_top", mode="append", properties=clickhouse_properties)

    customers_countries = customers.groupBy("country_id") \
        .agg(count("*").alias("count_customers")) \
        .join(
            countries.alias("countries"),
            customers.country_id == countries.id,
            "left"
        ).select(
            col("country_id"),
            col("countries.name").alias("country_name"),
            col("count_customers")
        )
    customers_countries.write.jdbc(url=ch_jdbc_url, table="customers_countries", mode="append", properties=clickhouse_properties)

    customers_average_check = sales.groupBy("customer_id") \
        .agg(avg("total_price").alias("average_check")) \
        .join(
            customers.alias("customers"),
            col("customer_id") == customers.id,
            "left"
        ).select(
            col("customer_id"),
            col("customers.first_name").alias("first_name"),
            col("customers.last_name").alias("last_name"),
            col("average_check")
        )
    customers_average_check.write.jdbc(url=ch_jdbc_url, table="customers_average_check", mode="append", properties=clickhouse_properties)


def DataMartTimes():
    # Месячные и годовые тренды продаж.

    sales_with_dates = sales.join(
        dates,
        sales.date_id == dates.id,
        "inner"
    )

    times_yearly_trends = sales_with_dates.groupBy(
        year("date").alias("year")
    ).agg(
        sum("total_price").alias("yearly_revenue"),
        sum("product_quantity").alias("yearly_quantity")
    ).orderBy("year")
    times_yearly_trends.write.jdbc(url=ch_jdbc_url, table="times_yearly_trends", mode="append", properties=clickhouse_properties)
    
    times_monthly_trends = sales_with_dates.groupBy(
        year("date").alias("year"),
        month("date").alias("month")
    ).agg(
        sum("total_price").alias("monthly_revenue"),
        sum("product_quantity").alias("monthly_quantity")
    ).orderBy("year", "month")
    times_monthly_trends.write.jdbc(url=ch_jdbc_url, table="times_monthly_trends", mode="append", properties=clickhouse_properties)

    # Сравнение выручки за разные периоды.

    sales_with_dates_with_year_months_quarters = sales_with_dates \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("quarter", quarter("date"))
    
    monthly_revenue = sales_with_dates_with_year_months_quarters.groupBy("year", "month") \
        .agg(sum("total_price").alias("monthly_revenue")) \
        .orderBy("year", "month")
    
    window_spec = Window.orderBy("year", "month")
    times_monthly_comparison = monthly_revenue.withColumn(
        "prev_month_revenue",
        lag("monthly_revenue", 1).over(window_spec)
    ).withColumn(
        "monthly_growth",
        (col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100
    ).na.fill(0)
    times_monthly_comparison.write.jdbc(url=ch_jdbc_url, table="times_monthly_comparison", mode="append", properties=clickhouse_properties)
    
    times_quarterly_revenue = sales_with_dates_with_year_months_quarters.groupBy("year", "quarter") \
        .agg(sum("total_price").alias("quarterly_revenue")) \
        .orderBy("year", "quarter")
    times_quarterly_revenue.write.jdbc(url=ch_jdbc_url, table="times_quarterly_revenue", mode="append", properties=clickhouse_properties)
    
    # Средний размер заказа по месяцам.

    times_avg_order_size = sales_with_dates_with_year_months_quarters.groupBy("year", "month") \
        .agg(
            avg("total_price").alias("avg_order_value"),
            avg("product_quantity").alias("avg_items_per_order"),
            count("*").alias("order_count")
        ) \
        .orderBy("year", "month")
    times_avg_order_size.write.jdbc(url=ch_jdbc_url, table="times_avg_order_size", mode="append", properties=clickhouse_properties)


def DataMartStores():
    # Топ-5 магазинов с наибольшей выручкой.

    stores_top = sales.groupBy("store_id") \
        .agg(sum("total_price").alias("total_price")) \
        .orderBy(desc("total_price")) \
        .limit(5) \
        .join(
            stores.alias("stores"),
            col("store_id") == stores.id,
            "left"
        ).select(
            col("store_id"),
            col("stores.name").alias("name"),
            col("total_price")
        )
    stores_top.write.jdbc(url=ch_jdbc_url, table="stores_top", mode="append", properties=clickhouse_properties)

    # Распределение продаж по городам и странам.

    sales_geo = sales.join(
        stores,
        sales.store_id == stores.id,
        "inner"
    ).join(
        cities,
        stores.city_id == cities.id,
        "inner"
    ).join(
        countries,
        stores.country_id == countries.id,
        "inner"
    )
    
    stores_sales_by_country = sales_geo.groupBy(
        "country_id",
        countries.name.alias("country_name")
    ).agg(
        sum("total_price").alias("total_revenue"),
        sum("product_quantity").alias("total_quantity"),
        count(sales.id).alias("order_count")
    ).orderBy(desc("total_revenue"))
    stores_sales_by_country.write.jdbc(url=ch_jdbc_url, table="stores_sales_by_country", mode="append", properties=clickhouse_properties)
    
    stores_sales_by_city = sales_geo.groupBy(
        "city_id",
        cities.name.alias("city_name"),
        "country_id",
        countries.name.alias("country_name")
    ).agg(
        sum("total_price").alias("total_revenue"),
        sum("product_quantity").alias("total_quantity"),
        count(sales.id).alias("order_count")
    ).orderBy(desc("total_revenue"))
    stores_sales_by_city.write.jdbc(url=ch_jdbc_url, table="stores_sales_by_city", mode="append", properties=clickhouse_properties)

    # Средний чек для каждого магазина.

    stores_avg_receipt = sales.join(
        stores,
        sales.store_id == stores.id,
        "inner"
    ).groupBy(
        "store_id",
        stores.name.alias("store_name"),
        stores.city_id.alias("city_id")
    ).agg(
        avg("total_price").alias("avg_receipt")
    ).orderBy(desc("avg_receipt"))
    stores_avg_receipt.write.jdbc(url=ch_jdbc_url, table="stores_avg_receipt", mode="append", properties=clickhouse_properties)


def DataMartSuppliers():
    # Топ-5 поставщиков с наибольшей выручкой.

    suppliers_top = sales.groupBy("supplier_id") \
        .agg(sum("total_price").alias("total_price")) \
        .orderBy(desc("total_price")) \
        .limit(5) \
        .join(
            suppliers.alias("suppliers"),
            col("supplier_id") == suppliers.id,
            "left"
        ).select(
            col("supplier_id"),
            col("suppliers.name").alias("name"),
            col("total_price")
        )
    suppliers_top.write.jdbc(url=ch_jdbc_url, table="suppliers_top", mode="append", properties=clickhouse_properties)

    # Средняя цена товаров от каждого поставщика.

    sales_with_price = sales.withColumn(
        "item_price",
        col("total_price") / col("product_quantity")
    )
    
    suppliers_avg_price_by_supplier = sales_with_price.join(
        suppliers,
        sales_with_price.supplier_id == suppliers.id,
        "inner"
    ).groupBy(
        "supplier_id",
        suppliers.name.alias("supplier_name")
    ).agg(
        avg("item_price").alias("avg_product_price")
    ).orderBy(desc("avg_product_price"))
    suppliers_avg_price_by_supplier.write.jdbc(url=ch_jdbc_url, table="suppliers_avg_price_by_supplier", mode="append", properties=clickhouse_properties)

    # Распределение продаж по странам поставщиков.

    sales_by_supplier_country = sales.join(
        suppliers,
        sales.supplier_id == suppliers.id,
        "inner"
    ).join(
        countries,
        suppliers.country_id == countries.id,
        "inner"
    )
    
    suppliers_distribution = sales_by_supplier_country.groupBy(
        "country_id",
        countries.name.alias("country_name")
    ).agg(
        sum("total_price").alias("total_revenue"),
        sum("product_quantity").alias("total_quantity"),
        count(sales.id).alias("order_count"),
        countDistinct("supplier_id").alias("supplier_count")
    ).orderBy(desc("total_revenue"))
    suppliers_distribution.write.jdbc(url=ch_jdbc_url, table="suppliers_distribution", mode="append", properties=clickhouse_properties)


def DataMartQuality():
    # Продукты с наивысшим и наименьшим рейтингом.

    quality_products_top_best = products.orderBy(desc("rating")).limit(100).select(
        col("id"),
        col("name"),
        col("rating")
    )
    quality_products_top_best.write.jdbc(url=ch_jdbc_url, table="quality_products_top_best", mode="append", properties=clickhouse_properties)

    quality_products_top_worst = products.orderBy(asc("rating")).limit(100).select(
        col("id"),
        col("name"),
        col("rating")
    )
    quality_products_top_worst.write.jdbc(url=ch_jdbc_url, table="quality_products_top_worst", mode="append", properties=clickhouse_properties)

    # Корреляция между рейтингом и объемом продаж.

    sales_by_product = sales.groupBy("product_id").agg(
        sum("product_quantity").alias("total_quantity_sold"),
        sum("total_price").alias("total_revenue")
    )
    
    product_stats = sales_by_product.join(
        products,
        sales_by_product.product_id == products.id,
        "inner"
    ).filter(col("rating").isNotNull())
    
    assembler = VectorAssembler(
        inputCols=["rating", "total_quantity_sold", "total_revenue"],
        outputCol="features"
    )
    assembled_data = assembler.transform(product_stats)
    
    corr_matrix = Correlation.corr(assembled_data, "features").collect()[0][0]
    
    rating_quantity_corr = corr_matrix.toArray()[0][1]
    rating_revenue_corr = corr_matrix.toArray()[0][2]
    
    # Альтернативный расчет через SQL функции
    sql_corr = product_stats.select(
        corr("rating", "total_quantity_sold").alias("rating_quantity_corr"),
        corr("rating", "total_revenue").alias("rating_revenue_corr")
    ).collect()[0]
    
    # Создаем итоговый отчет
    quality_correlation_report = spark.createDataFrame(
        [(
            "Rating vs Quantity Sold", 
            float(rating_quantity_corr), 
            float(sql_corr["rating_quantity_corr"])
         ),
         (
            "Rating vs Revenue", 
            float(rating_revenue_corr), 
            float(sql_corr["rating_revenue_corr"])
         )],
        ["metric", "pearson_correlation_matrix", "pearson_correlation_sql"]
    )
    quality_correlation_report.write.jdbc(url=ch_jdbc_url, table="quality_correlation_report", mode="append", properties=clickhouse_properties)

    # Продукты с наибольшим количеством отзывов.

    quality_products_top_reviews = products.orderBy(desc("reviews")).limit(100).select(
        col("id"),
        col("name"),
        col("reviews")
    )
    quality_products_top_reviews.write.jdbc(url=ch_jdbc_url, table="quality_products_top_reviews", mode="append", properties=clickhouse_properties)


DataMartProducts()
DataMartCustomers()
DataMartTimes()
DataMartStores()
DataMartSuppliers()
DataMartQuality()

print("Засыпаю")
sleep(2)
print("Просыпаюсь")

spark.stop()
