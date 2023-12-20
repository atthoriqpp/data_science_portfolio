-- ================= Product Category Analysis =======================================
-- This query evaluates the performance of products across various categories over the year, utilizing the following metrics:
-- 1. Revenue generated across years
-- 2. Total number of canceled orders across years
-- 3. Top-selling product for each year
-- 4. Product with the highest total cancellation for each year
-- ==================================================================================================
-- Revenue across years
WITH revenue AS (
	SELECT EXTRACT(YEAR FROM od.order_purchase_timestamp) AS year,
		   ROUND(SUM(oid.price::numeric + oid.freight_value::numeric), 2) AS revenue
	FROM orders_dataset AS od
	INNER JOIN order_items_dataset AS oid
	USING(order_id)
	WHERE od.order_status = 'delivered'
	GROUP BY 1
),
-- Total canceled order across years
total_canceled_order AS (
	SELECT EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
		   COUNT(DISTINCT order_id) AS canceled_order
	FROM orders_dataset
	WHERE order_status = 'canceled'
	GROUP BY 1
),
-- Top-selling product for each year
top_product AS (
	SELECT year,
		   product_category_name,
		   total_revenue
	FROM (
		SELECT DENSE_RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS ranking,
			   year,
			   product_category_name,
			   total_revenue
		FROM (
			SELECT year,
				   product_category_name,
				   ROUND(SUM(revenue)::numeric, 2) AS total_revenue
			FROM (
				SELECT EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
					   pd.product_category_name,
					   oid.price + oid.freight_value AS revenue
				FROM orders_dataset AS od
				INNER JOIN order_items_dataset AS oid
				USING(order_id)
				INNER JOIN products_dataset AS pd
				USING(product_id)
				WHERE od.order_status = 'delivered'
				) AS product_sales
			GROUP BY 1, 2
			) AS product_total_revenue
		) AS product_rank_revenue
	WHERE ranking = 1
),
-- Products with the highest total cancelation across years
top_canceled_product AS (
	SELECT year,
		   product_category_name,
		   cancel_count
	FROM(
		SELECT DENSE_RANK() OVER (PARTITION BY year ORDER BY cancel_count DESC) AS ranking,
			   year,
			   product_category_name,
			   cancel_count
		FROM (
			SELECT EXTRACT(YEAR FROM od.order_purchase_timestamp) AS year,
				   pd.product_category_name,
				   COUNT(*) AS cancel_count
			FROM orders_dataset AS od
			INNER JOIN order_items_dataset AS oid
			USING(order_id)
			INNER JOIN products_dataset AS pd
			USING(product_id)
			WHERE od.order_status = 'canceled'
			GROUP BY 1, 2
			) AS canceled_product_count
		) AS canceled_product_rank
	WHERE ranking = 1
)
-- Concatenating the result
SELECT r.year,
	   r.revenue AS total_revenue,
	   tco.canceled_order AS total_canceled_order,
	   tp.product_category_name AS top_selling_product,
	   tp.total_revenue AS top_selling_product_revenue,
	   tcp.product_category_name AS top_canceled_product,
	   tcp.cancel_count AS top_canceled_product_cancel_count 
FROM revenue AS r
INNER JOIN total_canceled_order AS tco
USING(year)
INNER JOIN top_product AS tp
USING(year)
INNER JOIN top_canceled_product AS tcp
USING(year);