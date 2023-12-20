-- ========== TABLE CREATION ==========
-- geolocation_dataset 
DROP TABLE IF EXISTS public.geolocation_dataset;

CREATE TABLE public.geolocation_dataset(
	geolocation_zip_code_prefix VARCHAR,  -- Can't set PK, duplicates exist
	geolocation_lat FLOAT8,
	geolocation_lng FLOAT8,
	geolocation_city VARCHAR,
	geolocation_state CHAR(5)
);

-- customers_dataset
DROP TABLE IF EXISTS public.customers_dataset;

CREATE TABLE public.customers_dataset(
	customer_id CHAR(50) PRIMARY KEY,
	customer_unique_id CHAR(50),
	customer_zip_code_prefix VARCHAR,
	customer_city VARCHAR,
	customer_state CHAR(5)
);

-- sellers_dataset
DROP TABLE IF EXISTS public.sellers_dataset;

CREATE TABLE public.sellers_dataset(
	seller_id CHAR(50) PRIMARY KEY,
	seller_zip_code_prefix VARCHAR,
	seller_city VARCHAR,
	seller_state CHAR(5)
);

-- products_dataset
DROP TABLE IF EXISTS public.products_dataset;

CREATE TABLE public.products_dataset(
	idx INT,
	product_id CHAR(50) PRIMARY KEY,
	product_category_name VARCHAR,
	product_name_length FLOAT8,
	product_description_length FLOAT8,
	product_photos_qty FLOAT8,
	product_weight_g FLOAT8,
	product_length_cm FLOAT8,
	product_height_cm FLOAT8,
	product_width_cm FLOAT8
);

-- order_items_dataset
DROP TABLE IF EXISTS public.order_items_dataset;

CREATE TABLE public.order_items_dataset(
	order_id CHAR(50),
	order_item_id CHAR(50), -- Can't set PK, duplicates exist
	product_id CHAR(50),
	seller_id CHAR(50),
	shipping_limit_date TIMESTAMP,
	price FLOAT8,
	freight_value FLOAT8
);

-- orders_dataset
DROP TABLE IF EXISTS public.orders_dataset;

CREATE TABLE public.orders_dataset(
	order_id CHAR(50) PRIMARY KEY,
	customer_id CHAR(50),
	order_status VARCHAR,
	order_purchase_timestamp TIMESTAMP,
	order_approved_at TIMESTAMP,
	order_delivered_carrier_date TIMESTAMP,
	order_delivered_customer_date TIMESTAMP,
	order_estimated_delivery_date TIMESTAMP
);

-- payments_dataset
DROP TABLE IF EXISTS public.payments_dataset;

CREATE TABLE public.payments_dataset(
	order_id CHAR(50),
	payment_sequential INT,
	payment_type VARCHAR,
	payment_installments INT,
	payment_value FLOAT8
);

-- reviews_dataset
DROP TABLE IF EXISTS public.reviews_dataset;

CREATE TABLE public.reviews_dataset(
	review_id CHAR(50), -- Can't set PK, duplicates exist
	order_id CHAR(50),
	review_score INT,
	review_comment_title VARCHAR,
	review_comment_message TEXT,
	review_creation_date TIMESTAMP,
	review_answer_timestamp TIMESTAMP
);

-- ========== DATA IMPORT ==========
-- geolocation_dataset
COPY public.geolocation_dataset(
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;

-- customers_dataset
COPY public.customers_dataset(
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

-- sellers_dataset
COPY public.sellers_dataset(
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

-- products_dataset
COPY public.products_dataset(
	idx,
	product_id,
	product_category_name,
	product_name_length,
	product_description_length,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\product_dataset.csv'
DELIMITER ','
CSV HEADER;

ALTER TABLE public.products_dataset
DROP COLUMN idx;

-- order_items_dataset
COPY public.order_items_dataset(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\order_items_dataset.csv'
DELIMITER ','
CSV HEADER;

-- orders_dataset
COPY public.orders_dataset(
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\orders_dataset.csv'
DELIMITER ','
CSV HEADER;

-- payments_dataset
COPY public.payments_dataset(
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

-- reviews_dataset
COPY public.reviews_dataset(
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
FROM 'D:\COURSES\Data Analytics\Rakamin\JAP\Projects\Analyzing E-Commerce Business Performance with SQL\DS\order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;

-- ========== NECESSARY CLEANING ==========
-- geolocation_dataset 
CREATE TABLE geolocation_clean_dataset AS 
WITH geolocation_clean AS (
	SELECT geolocation_zip_code_prefix,
		   geolocation_lat,
		   geolocation_lng,
		   geolocation_city,
		   geolocation_state
	FROM (
		SELECT ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix) AS rn,
			   *
		FROM geolocation_dataset
	) AS geo_tmp
	WHERE rn = 1
),
geolocation_customer_clean AS (
	SELECT customer_zip_code_prefix,
		   geolocation_lat,
		   geolocation_lng,
		   customer_city,
		   customer_state
	FROM(
		SELECT ROW_NUMBER() OVER (PARTITION BY customer_zip_code_prefix) AS rn,
			   *
		FROM (
			SELECT c.customer_zip_code_prefix,
				   g.geolocation_lat,
				   g.geolocation_lng,
				   c.customer_city,
				   c.customer_state
			FROM customers_dataset AS c
			LEFT JOIN geolocation_dataset AS g
			ON c.customer_city = g.geolocation_city
			AND c.customer_state = g.geolocation_state
			WHERE c.customer_zip_code_prefix NOT IN (
				SELECT geolocation_zip_code_prefix
				FROM geolocation_dataset
			)
		) AS cust_geo
	) AS cust_geo_tmp 
	WHERE rn = 1
),
geolocation_seller_clean AS (
	SELECT seller_zip_code_prefix,
		   geolocation_lat,
		   geolocation_lng,
		   seller_city,
		   seller_state
	FROM(
		SELECT ROW_NUMBER() OVER (PARTITION BY seller_zip_code_prefix) AS rn,
			   *
		FROM (
			SELECT s.seller_zip_code_prefix,
				   g.geolocation_lat,
				   g.geolocation_lng,
				   s.seller_city,
				   s.seller_state
			FROM sellers_dataset AS s
			LEFT JOIN geolocation_dataset AS g
			ON s.seller_city = g.geolocation_city
			AND s.seller_state = g.geolocation_state
			WHERE s.seller_zip_code_prefix NOT IN(
				SELECT geolocation_zip_code_prefix
				FROM geolocation_dataset
				UNION
				SELECT customer_zip_code_prefix
				FROM geolocation_customer_clean
			)
		) AS seller_geo
	) AS seller_geo_tmp
	WHERE rn = 1
)
SELECT *
FROM geolocation_clean
UNION 
SELECT *
FROM geolocation_customer_clean
UNION
SELECT *
FROM geolocation_seller_clean;


-- ========== ADDING REQUIRED PRIMARY KEYS AND FOREIGN KEYS ==========
-- Primary key: geolocation_clean_dataset 
ALTER TABLE geolocation_clean_dataset
ADD CONSTRAINT geolocation_pkey PRIMARY KEY (geolocation_zip_code_prefix);

-- Foreign key: customers_dataset & geolocation_clean_dataset
ALTER TABLE customers_dataset
ADD CONSTRAINT customers_fk_geolocation
FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation_clean_dataset (geolocation_zip_code_prefix)
ON DELETE CASCADE -- This ensures data integrity if there's a deletion in the geolocation_zip_code_prefix, the corresponding customer_zip_code_prefix will be deleted too 
ON UPDATE CASCADE; -- This ensures data integrity if there's a value update in the geolocation_zip_code_prefix, the corresponding customer_zip_code_prefix value will be updated too

-- Foreign key: sellers_dataset & geolocation_clean_dataset
ALTER TABLE sellers_dataset
ADD CONSTRAINT sellers_fk_geolocation
FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation_clean_dataset (geolocation_zip_code_prefix)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: order_items_dataset & sellers_dataset
ALTER TABLE order_items_dataset
ADD CONSTRAINT order_items_fk_sellers
FOREIGN KEY (seller_id) REFERENCES sellers_dataset (seller_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: order_items_dataset & products_dataset
ALTER TABLE order_items_dataset
ADD CONSTRAINT order_items_fk_products
FOREIGN KEY (product_id) REFERENCES products_dataset (product_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: order_items_dataset & orders_dataset
ALTER TABLE order_items_dataset
ADD CONSTRAINT order_items_fk_orders
FOREIGN KEY (order_id) REFERENCES orders_dataset (order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: orders_dataset & customers_dataset
ALTER TABLE orders_dataset
ADD CONSTRAINT orders_fk_customers
FOREIGN KEY (customer_id) REFERENCES customers_dataset (customer_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: payments_dataset & orders_dataset
ALTER TABLE payments_dataset
ADD CONSTRAINT payments_fk_orders
FOREIGN KEY (order_id) REFERENCES orders_dataset (order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Foreign key: reviews_dataset & orders_dataset
ALTER TABLE reviews_dataset
ADD CONSTRAINT reviews_fk_orders
FOREIGN KEY (order_id) REFERENCES orders_dataset (order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;