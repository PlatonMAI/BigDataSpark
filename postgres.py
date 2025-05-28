from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, monotonically_increasing_id
from time import sleep

def create_tables(spark, mock_data_df, pg_jdbc_url, properties):
    # Создание таблицы стран
    countries = mock_data_df.select(
        col("customer_country").alias("name")
    ).union(
        mock_data_df.select(col("seller_country").alias("name"))
    ).union(
        mock_data_df.select(col("store_country").alias("name"))
    ).union(
        mock_data_df.select(col("supplier_country").alias("name"))
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    countries.write.jdbc(url=pg_jdbc_url, table="d_countries", mode="overwrite", properties=properties)
    countries = spark.read.jdbc(url=pg_jdbc_url, table="d_countries", properties=properties)
    
    # Создание таблицы городов
    cities = mock_data_df.select(
        col("store_city").alias("name")
    ).union(
        mock_data_df.select(col("supplier_city").alias("name"))
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    cities.write.jdbc(url=pg_jdbc_url, table="d_cities", mode="overwrite", properties=properties)
    cities = spark.read.jdbc(url=pg_jdbc_url, table="d_cities", properties=properties)
    
    # Создание таблицы дат
    dates = mock_data_df.select(
        to_date(col("sale_date"), "M/d/y").alias("date")
    ).union(
        mock_data_df.select(to_date(col("product_release_date"), "M/d/y").alias("date"))
    ).union(
        mock_data_df.select(to_date(col("product_expiry_date"), "M/d/y").alias("date"))
    ).distinct().filter("date IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    dates.write.jdbc(url=pg_jdbc_url, table="d_dates", mode="overwrite", properties=properties)
    dates = spark.read.jdbc(url=pg_jdbc_url, table="d_dates", properties=properties)
    
    # Создание таблицы типов питомцев
    pet_types = mock_data_df.select(
        col("customer_pet_type").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    pet_types.write.jdbc(url=pg_jdbc_url, table="d_pet_types", mode="overwrite", properties=properties)
    pet_types = spark.read.jdbc(url=pg_jdbc_url, table="d_pet_types", properties=properties)
    
    # # Создание таблицы пород питомцев
    pet_breeds = mock_data_df.select(
        col("customer_pet_breed").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    pet_breeds.write.jdbc(url=pg_jdbc_url, table="d_pet_breeds", mode="overwrite", properties=properties)
    pet_breeds = spark.read.jdbc(url=pg_jdbc_url, table="d_pet_breeds", properties=properties)
    
    # # Создание таблицы категорий питомцев
    pet_categories = mock_data_df.select(
        col("pet_category").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    pet_categories.write.jdbc(url=pg_jdbc_url, table="d_pet_categories", mode="overwrite", properties=properties)
    pet_categories = spark.read.jdbc(url=pg_jdbc_url, table="d_pet_categories", properties=properties)
    
    # Создание таблицы питомцев
    pets = mock_data_df.join(
        pet_types.alias("pt"),
        mock_data_df.customer_pet_type == pet_types.name,
        "left"
    ).join(
        pet_breeds.alias("pb"),
        mock_data_df.customer_pet_breed == pet_breeds.name,
        "left"
    ).join(
        pet_categories.alias("pc"),
        mock_data_df.pet_category == pet_categories.name,
        "left"
    ).select(
        col("pt.id").alias("type_id"),
        col("customer_pet_name").alias("name"),
        col("pb.id").alias("breed_id"),
        col("pc.id").alias("category_id")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    pets.write.jdbc(url=pg_jdbc_url, table="d_pets", mode="overwrite", properties=properties)
    pets = spark.read.jdbc(url=pg_jdbc_url, table="d_pets", properties=properties)
    
    # Создание таблицы категорий продуктов
    product_categories = mock_data_df.select(
        col("product_category").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    product_categories.write.jdbc(url=pg_jdbc_url, table="d_product_categories", mode="overwrite", properties=properties)
    product_categories = spark.read.jdbc(url=pg_jdbc_url, table="d_product_categories", properties=properties)
    
    # Создание таблицы цветов продуктов
    product_colors = mock_data_df.select(
        col("product_color").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    product_colors.write.jdbc(url=pg_jdbc_url, table="d_product_colors", mode="overwrite", properties=properties)
    product_colors = spark.read.jdbc(url=pg_jdbc_url, table="d_product_colors", properties=properties)
    
    # Создание таблицы размеров продуктов
    product_sizes = mock_data_df.select(
        col("product_size").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    product_sizes.write.jdbc(url=pg_jdbc_url, table="d_product_sizes", mode="overwrite", properties=properties)
    product_sizes = spark.read.jdbc(url=pg_jdbc_url, table="d_product_sizes", properties=properties)
    
    # Создание таблицы брендов продуктов
    product_brands = mock_data_df.select(
        col("product_brand").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    product_brands.write.jdbc(url=pg_jdbc_url, table="d_product_brands", mode="overwrite", properties=properties)
    product_brands = spark.read.jdbc(url=pg_jdbc_url, table="d_product_brands", properties=properties)
    
    # Создание таблицы материалов продуктов
    product_materials = mock_data_df.select(
        col("product_material").alias("name")
    ).distinct().filter("name IS NOT NULL").withColumn("id", monotonically_increasing_id() + 1)
    product_materials.write.jdbc(url=pg_jdbc_url, table="d_product_materials", mode="overwrite", properties=properties)
    product_materials = spark.read.jdbc(url=pg_jdbc_url, table="d_product_materials", properties=properties)
    
    # Создание таблицы продуктов
    products = mock_data_df.join(
        product_categories.alias("pc"),
        mock_data_df.product_category == product_categories.name,
        "left"
    ).join(
        product_colors.alias("pcol"),
        mock_data_df.product_color == product_colors.name,
        "left"
    ).join(
        product_sizes.alias("ps"),
        mock_data_df.product_size == product_sizes.name,
        "left"
    ).join(
        product_brands.alias("pb"),
        mock_data_df.product_brand == product_brands.name,
        "left"
    ).join(
        product_materials.alias("pm"),
        mock_data_df.product_material == product_materials.name,
        "left"
    ).join(
        dates.alias("rd"),
        to_date(mock_data_df.product_release_date, "M/d/y") == col("rd.date"),
        "left"
    ).join(
        dates.alias("ed"),
        to_date(mock_data_df.product_expiry_date, "M/d/y") == col("ed.date"),
        "left"
    ).select(
        col("product_name").alias("name"),
        col("pc.id").alias("category_id"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        col("pcol.id").alias("color_id"),
        col("ps.id").alias("size_id"),
        col("pb.id").alias("brand_id"),
        col("pm.id").alias("material_id"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("rd.id").alias("release_date_id"),
        col("ed.id").alias("expiry_date_id")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    products.write.jdbc(url=pg_jdbc_url, table="d_products", mode="overwrite", properties=properties)
    products = spark.read.jdbc(url=pg_jdbc_url, table="d_products", properties=properties)
    
    # Создание таблицы клиентов
    customers = mock_data_df.join(
        countries.alias("c"),
        mock_data_df.customer_country == col("c.name"),
        "left"
    ).join(
        pet_types.alias("pt"),
        mock_data_df.customer_pet_type == pet_types.name,
        "left"
    ).join(
        pet_breeds.alias("pb"),
        mock_data_df.customer_pet_breed == pet_breeds.name,
        "left"
    ).join(
        pet_categories.alias("pc"),
        mock_data_df.pet_category == pet_categories.name,
        "left"
    ).join(
        pets.alias("p"),
        (mock_data_df.customer_pet_name == col("p.name")) &
        (col("pt.id") == col("p.type_id")) &
        (col("pb.id") == col("p.breed_id")) &
        (col("pc.id") == col("p.category_id")),
        "left"
    ).select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("c.id").alias("country_id"),
        col("customer_postal_code").alias("postal_code"),
        col("p.id").alias("pet_id")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    customers.write.jdbc(url=pg_jdbc_url, table="d_customers", mode="overwrite", properties=properties)
    customers = spark.read.jdbc(url=pg_jdbc_url, table="d_customers", properties=properties)
    
    # Создание таблицы продавцов
    sellers = mock_data_df.join(
        countries.alias("c"),
        mock_data_df.seller_country == col("c.name"),
        "left"
    ).select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("c.id").alias("country_id"),
        col("seller_postal_code").alias("postal_code")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    sellers.write.jdbc(url=pg_jdbc_url, table="d_sellers", mode="overwrite", properties=properties)
    sellers = spark.read.jdbc(url=pg_jdbc_url, table="d_sellers", properties=properties)
    
    # Создание таблицы магазинов
    stores = mock_data_df.join(
        countries.alias("co"),
        mock_data_df.store_country == col("co.name"),
        "left"
    ).join(
        cities.alias("ci"),
        mock_data_df.store_city == col("ci.name"),
        "left"
    ).select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("ci.id").alias("city_id"),
        col("store_state").alias("state"),
        col("co.id").alias("country_id"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    stores.write.jdbc(url=pg_jdbc_url, table="d_stores", mode="overwrite", properties=properties)
    stores = spark.read.jdbc(url=pg_jdbc_url, table="d_stores", properties=properties)
    
    # Создание таблицы поставщиков
    suppliers = mock_data_df.join(
        countries.alias("co"),
        mock_data_df.supplier_country == col("co.name"),
        "left"
    ).join(
        cities.alias("ci"),
        mock_data_df.supplier_city == col("ci.name"),
        "left"
    ).select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("ci.id").alias("city_id"),
        col("co.id").alias("country_id")
    ).distinct().withColumn("id", monotonically_increasing_id() + 1)
    suppliers.write.jdbc(url=pg_jdbc_url, table="d_suppliers", mode="overwrite", properties=properties)
    suppliers = spark.read.jdbc(url=pg_jdbc_url, table="d_suppliers", properties=properties)
    
    # Создание таблицы фактов продаж
    sales = mock_data_df.join(
        customers.alias("cu"),
        mock_data_df.customer_email == col("cu.email"),
        "left"
    ).join(
        sellers.alias("s"),
        mock_data_df.seller_email == col("s.email"),
        "left"
    ).join(
        products.alias("p"),
        (mock_data_df.product_name == col("p.name")) &
        (mock_data_df.product_price == col("p.price")) &
        (mock_data_df.product_weight == col("p.weight")) &
        (mock_data_df.product_description == col("p.description")) &
        (mock_data_df.product_rating == col("p.rating")) &
        (mock_data_df.product_reviews == col("p.reviews")),
        "left"
    ).join(
        dates.alias("d"),
        to_date(mock_data_df.sale_date, "M/d/y") == col("d.date"),
        "left"
    ).join(
        stores.alias("st"),
        mock_data_df.store_email == col("st.email"),
        "left"
    ).join(
        suppliers.alias("su"),
        mock_data_df.supplier_email == col("su.email"),
        "left"
    ).select(
        col("cu.id").alias("customer_id"),
        col("s.id").alias("seller_id"),
        col("p.id").alias("product_id"),
        col("product_quantity").alias("product_quantity"),
        col("d.id").alias("date_id"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price"),
        col("st.id").alias("store_id"),
        col("su.id").alias("supplier_id")
    ).withColumn("id", monotonically_increasing_id() + 1)
    sales.write.jdbc(url=pg_jdbc_url, table="f_sales", mode="overwrite", properties=properties)
    sales = spark.read.jdbc(url=pg_jdbc_url, table="f_sales", properties=properties)

def main():
    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("Spark SQL with PostgreSQL and ClickHouse") \
        .config("spark.jars", "postgresql-42.6.0.jar,clickhouse-jdbc-0.4.6.jar") \
        .getOrCreate()
    
    pg_jdbc_url = "jdbc:postgresql://localhost:5432/spark_db"
    properties = {
        "user": "spark_user",
        "password": "spark_password",
        "driver": "org.postgresql.Driver"
    }
    table_src = "mock_data"
    
    # Загрузка исходных данных
    mock_data_df = spark.read.jdbc(url=pg_jdbc_url, table=table_src, properties=properties)
    
    create_tables(spark, mock_data_df, pg_jdbc_url, properties)

    print("Засыпаю")
    sleep(2)
    print("Просыпаюсь")
    
    # Останавливаем SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
