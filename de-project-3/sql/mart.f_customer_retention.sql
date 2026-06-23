TRUNCATE mart.f_customer_retention;
WITH up_sales AS (
SELECT
	*,
	count(customer_id) FILTER (
	WHERE status = 'shipped') OVER (PARTITION BY customer_id,
	period_id) AS new_customers_1
FROM
	(
	SELECT
		id,
		f_sales.date_id,
		item_id,
		customer_id,
		payment_amount,
		status,
		date_actual,
		week_of_year AS period_id,
		('Неделя с ' || first_day_of_week || ' по ' || last_day_of_week) AS period_name,
		city_id,
		quantity
	FROM
		mart.f_sales
	JOIN mart.d_calendar ON
		f_sales.date_id = d_calendar.date_id) t),
cte_refunded_customer_count AS (
SELECT
		period_id,
		item_id,
		count (customer_id) AS refunded_customer_count
FROM
	(
	SELECT
		customer_id,
		item_id,
		period_id
	FROM
		up_sales
	WHERE
		status = 'refunded'
	GROUP BY
		period_id,
		item_id,
		customer_id) t
GROUP BY
		period_id,
		item_id),
cte_up_sales AS (
SELECT  		
		period_id,
		period_name,
		item_id,
		sum(CASE WHEN new_customers_1 = 1 THEN 1 END) AS new_customers_count,
		sum(CASE WHEN new_customers_1 > 1 THEN 1 END) AS returning_customers_count,
		SUM(CASE WHEN status = 'shipped' AND new_customers_1 = 1 THEN payment_amount END) AS new_customers_revenue,
		sum(CASE WHEN status = 'shipped' AND new_customers_1 > 1 THEN payment_amount END) AS returning_customers_revenue,
		sum(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS customers_refunded
FROM
	up_sales
GROUP BY
	period_id,
	period_name,
	item_id)
INSERT
	INTO
	mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count, 
	period_name,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded)
SELECT 
	COALESCE (new_customers_count, 0) AS new_customers_count,
	COALESCE (returning_customers_count, 0) AS returning_customers_count,
	COALESCE (refunded_customer_count, 0) AS refunded_customer_count,
	period_name,
	period_id,
	item_id,
	COALESCE (new_customers_revenue, 0) AS new_customers_revenue,
	COALESCE (returning_customers_revenue, 0) AS returning_customers_revenue,
	COALESCE (customers_refunded, 0) AS customers_refunded
FROM
	cte_up_sales
LEFT JOIN cte_refunded_customer_count AS cte
		USING (period_id,
				item_id)
;
