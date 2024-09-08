TRUNCATE public.shipping_country_rates RESTART IDENTITY CASCADE;
INSERT
	INTO
	public.shipping_country_rates (
		shipping_country,
		shipping_country_base_rate)
SELECT
	DISTINCT 
	shipping_country,
	shipping_country_base_rate
FROM
	public.shipping;

INSERT
	INTO shipping_agreement
SELECT DISTINCT 
	vad[1] :: int AS agreementid, 
	vad[2] AS agreement_number, 
	vad[3] :: numeric(14, 3) AS agreement_rate, 
	vad[4] :: numeric(14, 3) AS agreement_commission
FROM 
	(
	SELECT
		regexp_split_to_array(vendor_agreement_description, ':') AS vad
	FROM
		public.shipping) AS t
	ORDER BY
	agreementid;

INSERT
	INTO
	shipping_transfer (transfer_type,
						transfer_model,
						shipping_transfer_rate)
SELECT
	DISTINCT 
	std[1] AS transfer_type, 
	std[2] AS transfer_model, 
	shipping_transfer_rate
FROM
	(
	SELECT
		DISTINCT 
	regexp_split_to_array(shipping_transfer_description , ':') AS std,
		shipping_transfer_rate
	FROM
		public.shipping) AS t;
	
INSERT
	INTO
	shipping_info
SELECT
	DISTINCT 
	shippingid,
	shipping_plan_datetime ,
	payment_amount ,
	vendorid,
	shipping_country_id,
	transfer_type_id,
	(regexp_split_to_array(vendor_agreement_description, ':'))[1] :: bigint  AS agreementid
FROM
	public.shipping
LEFT JOIN public.shipping_country_rates 
		USING (shipping_country)
LEFT JOIN public.shipping_transfer ON 
		shipping_transfer_description = concat_ws(':', transfer_type, transfer_model);
	
INSERT
	INTO
	shipping_status
SELECT DISTINCT 
	s.shippingid,
	LAST_VALUE(status) OVER (PARTITION BY s.shippingid
ORDER BY
	state_datetime ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS status,
	LAST_VALUE(state) OVER (PARTITION BY s.shippingid
ORDER BY
	state_datetime ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS state,
	shipping_start_fact_datetime,
	shipping_end_fact_datetime
FROM shipping s
LEFT JOIN (SELECT shippingid, state_datetime AS shipping_start_fact_datetime
FROM shipping
WHERE state = 'booked') s1
USING (shippingid)
LEFT JOIN (SELECT shippingid, state_datetime AS shipping_end_fact_datetime
FROM shipping
WHERE state = 'recieved') s2
USING (shippingid);



	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	