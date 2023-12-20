-- ================= Yearly Customer Activity Growth Analysis =======================================
-- In this query, we measure the customer activity growth across the year using the following metrics:
-- 1. Average active users 
-- 2. New customer count
-- 3. Repeat customer count
-- 4. Average orders per user
-- ==================================================================================================
WITH yearly_avg_user_active AS (
	-- Average monthly active users across years
	SELECT year,
		   ROUND(AVG(user_count)) AS avg_monthly_user
	FROM (
		SELECT EXTRACT(MONTH FROM order_purchase_timestamp) AS month, 
			   EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
			   COUNT(DISTINCT customer_id) AS user_count
		FROM orders_dataset
		GROUP BY month, year
		ORDER BY year
		) AS monthly_count
	GROUP BY year
	ORDER BY year
), 
new_cust_count AS (
	-- Count of new customers each year
	SELECT year, COUNT(DISTINCT customer_unique_id) AS new_customers
	FROM (
		SELECT EXTRACT(YEAR FROM ord.order_purchase_timestamp) AS year,
					   cust.customer_unique_id,
					   COUNT(DISTINCT ord.order_id) AS order_count
		FROM orders_dataset AS ord
		INNER JOIN customers_dataset AS cust
		USING(customer_id)
		GROUP BY year, cust.customer_unique_id
		HAVING COUNT(ord.order_id) = 1
		) AS new_cust
	GROUP BY year
	ORDER BY year
),
repeat_cust_count AS (
	-- Count of repeat customers each year
	SELECT year, COUNT(DISTINCT customer_unique_id) AS repeat_customers
	FROM (
		SELECT EXTRACT(YEAR FROM ord.order_purchase_timestamp) AS year,
					   cust.customer_unique_id,
					   COUNT(DISTINCT ord.order_id) AS order_count
		FROM orders_dataset AS ord
		INNER JOIN customers_dataset AS cust
		USING(customer_id)
		GROUP BY year, cust.customer_unique_id
		HAVING COUNT(ord.order_id) > 1
		) AS repeat_cust
	GROUP BY year
	ORDER BY year
),
yearly_avg_order AS (
	-- Average order by customer across years
	SELECT year, ROUND(AVG(order_count), 2) AS avg_order_per_cust
	FROM (
		SELECT EXTRACT(YEAR FROM ord.order_purchase_timestamp) AS year,
			   cust.customer_unique_id,
			   COUNT(DISTINCT ord.order_id) AS order_count
		FROM orders_dataset AS ord
		INNER JOIN customers_dataset AS cust
		USING(customer_id)
		GROUP BY year, cust.customer_unique_id
		ORDER BY order_count DESC
		) AS monthly_customer_order_count
	GROUP BY year
	ORDER BY year)
SELECT m1.year, m1.avg_monthly_user, m2.new_customers, m3.repeat_customers, m4.avg_order_per_cust
FROM yearly_avg_user_active AS m1
INNER JOIN new_cust_count AS m2
USING(year)
INNER JOIN repeat_cust_count AS m3
USING(year)
INNER JOIN yearly_avg_order AS m4
USING(year);

-- Monthly cust. activity
SELECT EXTRACT(YEAR FROM order_purchase_timestamp) AS yr,
	   EXTRACT(MONTH FROM order_purchase_timestamp) AS mo,
	   COUNT(DISTINCT customer_id) AS active_users
FROM orders_dataset
GROUP BY 1, 2;

-- Denormalized data for dashboard
SELECT *,
	   CASE WHEN order_count = 1 THEN 1
	   ELSE 0 END AS new_customer_flag,
	   CASE WHEN order_count > 1 THEN 1
	   ELSE 0 END AS repeat_customer_flag,
	   ROUND((price * freight_value)::numeric, 2) AS revenue
FROM (
	SELECT customer_unique_id, 
		   COUNT(DISTINCT order_id) AS order_count
	FROM customers_dataset
	INNER JOIN orders_dataset
	USING(customer_id)
	GROUP BY 1
	ORDER BY 2 DESC
	) AS t1
INNER JOIN customers_dataset
USING(customer_unique_id)
INNER JOIN orders_dataset
USING(customer_id)
INNER JOIN order_items_dataset
USING(order_id)
INNER JOIN products_dataset
USING(product_id)
INNER JOIN payments_dataset
USING(order_id);