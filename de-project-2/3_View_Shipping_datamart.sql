CREATE OR REPLACE
VIEW shipping_datamart AS 
SELECT
	shippingid,
	vendorid,
	transfer_type,
	date_part('day', shipping_end_fact_datetime - shipping_start_fact_datetime) AS full_day_at_shipping,
	CASE
		WHEN shipping_end_fact_datetime > shipping_plan_datetime
	THEN 1
		ELSE 0
	END AS is_delay,
	CASE
		WHEN status = 'finished'
	THEN 1
		ELSE 0
	END AS is_shipping_finish,
	CASE
		WHEN shipping_end_fact_datetime > shipping_plan_datetime --AND shipping_end_fact_datetime IS NOT NULL 
	THEN date_part('day', shipping_end_fact_datetime - shipping_plan_datetime)
		ELSE 0
	END AS delay_day_at_shipping,
	payment_amount,
	payment_amount * ( shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat,
	payment_amount * agreement_commission AS profit
FROM
	shipping_info
LEFT JOIN shipping_transfer
		USING (transfer_type_id)
LEFT JOIN
shipping_status
		USING (shippingid)
LEFT JOIN
shipping_country_rates
		USING (shipping_country_id)	
LEFT JOIN
shipping_agreement
		USING (agreementid)	
