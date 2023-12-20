-- ================= Payment Type Usage Analysis =======================================
-- This query evaluates the trend of payment method usage over the year, utilizing the following metrics:
-- 1. Most used payment method all time
-- 2. Most used payment method across years
-- ==================================================================================================
-- Top used payment method all time
SELECT payment_type,
	   COUNT(*) AS total_used
FROM payments_dataset
WHERE payment_type != 'not_defined'
GROUP BY 1
ORDER BY 2 DESC;

-- Trend of most used payment method across years
SELECT year,
	   payment_type,
	   total_used
FROM (
	SELECT year,
		   payment_type,
		   total_used,
		   DENSE_RANK() OVER (PARTITION BY year ORDER BY total_used DESC) AS ranking
	FROM (
		SELECT EXTRACT(YEAR FROM od.order_purchase_timestamp) AS year,
			   pd.payment_type,
			   COUNT(*) AS total_used
		FROM orders_dataset AS od
		INNER JOIN payments_dataset AS pd
		USING(order_id)
		WHERE payment_type != 'not_defined'
		GROUP BY 1, 2
		ORDER BY 1
		) AS total_method_used
	ORDER BY 1
	) AS method_ranked
WHERE ranking = 1;