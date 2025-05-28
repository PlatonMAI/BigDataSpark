-- DataMartProducts

CREATE TABLE default.products_top
(
    product_id Int64,
    product_name String,
    total_quantity_sold Int64
)
ENGINE = MergeTree()
ORDER BY (total_quantity_sold);

CREATE TABLE default.products_total_revenue
(
    category_id Int64,
    category_name String,
    total_revenue Int64
)
ENGINE = MergeTree()
ORDER BY (total_revenue);


-- DataMartCustomers

CREATE TABLE default.customers_top
(
    customer_id Int64,
    first_name String,
    last_name String,
    total_price Float64
)
ENGINE = MergeTree()
ORDER BY (total_price);

CREATE TABLE default.customers_countries
(
    country_id Int64,
    country_name String,
    count_customers Int64
)
ENGINE = MergeTree()
ORDER BY (count_customers);

CREATE TABLE default.customers_average_check
(
    customer_id Int64,
    first_name String,
    last_name String,
    average_check Float64
)
ENGINE = MergeTree()
ORDER BY (average_check);


-- DataMartCustomers

-- Месячные и годовые тренды продаж.

CREATE TABLE default.times_yearly_trends
(
    year Int32,
    yearly_revenue Float64,
    yearly_quantity Int64
)
ENGINE = MergeTree()
ORDER BY (year);

CREATE TABLE default.times_monthly_trends
(
    year Int32,
    month Int32,
    monthly_revenue Float64,
    monthly_quantity Int64
)
ENGINE = MergeTree()
ORDER BY (year, month);

-- Сравнение выручки за разные периоды.

CREATE TABLE default.times_monthly_comparison
(
    year Int32,
    month Int32,
    monthly_revenue Float64,
    prev_month_revenue Float64,
    monthly_growth Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE default.times_quarterly_revenue
(
    year Int32,
    quarter Int32,
    quarterly_revenue Float64
)
ENGINE = MergeTree()
ORDER BY (year, quarter);

-- Средний размер заказа по месяцам.

CREATE TABLE default.times_avg_order_size
(
    year Int32,
    month Int32,
    avg_order_value Float64,
    avg_items_per_order Float64,
    order_count Int32
)
ENGINE = MergeTree()
ORDER BY (year, month);


-- DataMartStores

-- Топ-5 магазинов с наибольшей выручкой.

CREATE TABLE default.stores_top
(
    store_id Int64,
    name String,
    total_price Float64
)
ENGINE = MergeTree()
ORDER BY (total_price);

-- Распределение продаж по городам и странам.

CREATE TABLE default.stores_sales_by_country
(
    country_id Int64,
    country_name String,
    total_revenue Float64,
    total_quantity Int64,
    order_count Int64
)
ENGINE = MergeTree()
ORDER BY (total_revenue);

CREATE TABLE default.stores_sales_by_city
(
    country_id Int64,
    country_name String,
    city_id Int64,
    city_name String,
    total_revenue Float64,
    total_quantity Int64,
    order_count Int64
)
ENGINE = MergeTree()
ORDER BY (total_revenue);

-- Средний чек для каждого магазина.

CREATE TABLE default.stores_avg_receipt
(
    store_id Int64,
    store_name String,
    city_id Int64,
    avg_receipt Float64
)
ENGINE = MergeTree()
ORDER BY (avg_receipt);


-- DataMartSuppliers

-- Топ-5 поставщиков с наибольшей выручкой.

CREATE TABLE default.suppliers_top
(
    supplier_id Int64,
    name String,
    total_price Float64
)
ENGINE = MergeTree()
ORDER BY (total_price);

-- Средняя цена товаров от каждого поставщика.

CREATE TABLE default.suppliers_avg_price_by_supplier
(
    supplier_id Int64,
    supplier_name String,
    avg_product_price Float64
)
ENGINE = MergeTree()
ORDER BY (avg_product_price);

-- Распределение продаж по странам поставщиков.

CREATE TABLE default.suppliers_distribution
(
    country_id Int64,
    country_name String,
    total_revenue Float64,
    total_quantity Int64,
    order_count Int64,
    supplier_count Int64
)
ENGINE = MergeTree()
ORDER BY (total_revenue);


-- DataMartQuality

-- Продукты с наивысшим и наименьшим рейтингом.

CREATE TABLE default.quality_products_top_best
(
    id Int64,
    name String,
    rating Float64
)
ENGINE = MergeTree()
ORDER BY (rating);

CREATE TABLE default.quality_products_top_worst
(
    id Int64,
    name String,
    rating Float64
)
ENGINE = MergeTree()
ORDER BY (rating);

-- Корреляция между рейтингом и объемом продаж.

CREATE TABLE default.quality_correlation_report
(
    metric String,
    pearson_correlation_matrix Float64,
    pearson_correlation_sql Float64
)
ENGINE = MergeTree()
ORDER BY (pearson_correlation_matrix, pearson_correlation_sql);

-- Продукты с наибольшим количеством отзывов.

CREATE TABLE default.quality_products_top_reviews
(
    id Int64,
    name String,
    reviews Int64
)
ENGINE = MergeTree()
ORDER BY (reviews);
