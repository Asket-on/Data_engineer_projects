-- 0. Clean target tables before loading to ensure idempotency and prevent duplicate key errors
TRUNCATE public.shipping_info CASCADE;
TRUNCATE public.shipping_status CASCADE;
TRUNCATE public.shipping_country_rates RESTART IDENTITY CASCADE;
TRUNCATE public.shipping_agreement CASCADE;
TRUNCATE public.shipping_transfer RESTART IDENTITY CASCADE;

-- 1. Load shipping_country_rates
INSERT INTO public.shipping_country_rates (
    shipping_country,
    shipping_country_base_rate
)
SELECT DISTINCT 
    shipping_country,
    shipping_country_base_rate
FROM public.shipping;

-- 2. Load shipping_agreement
INSERT INTO public.shipping_agreement (
    agreementid,
    agreement_number,
    agreement_rate,
    agreement_commission
)
SELECT DISTINCT 
    vad[1]::int AS agreementid, 
    vad[2] AS agreement_number, 
    vad[3]::numeric(14, 3) AS agreement_rate, 
    vad[4]::numeric(14, 3) AS agreement_commission
FROM (
    SELECT regexp_split_to_array(vendor_agreement_description, ':') AS vad
    FROM public.shipping
) AS t
ORDER BY agreementid;

-- 3. Load shipping_transfer
INSERT INTO public.shipping_transfer (
    transfer_type,
    transfer_model,
    shipping_transfer_rate
)
SELECT DISTINCT 
    std[1] AS transfer_type, 
    std[2] AS transfer_model, 
    shipping_transfer_rate
FROM (
    SELECT DISTINCT 
        regexp_split_to_array(shipping_transfer_description , ':') AS std,
        shipping_transfer_rate
    FROM public.shipping
) AS t;

-- 4. Load shipping_info
INSERT INTO public.shipping_info (
    shippingid,
    shipping_plan_datetime,
    payment_amount,
    vendorid,
    shipping_country_id,
    transfer_type_id,
    agreementid
)
SELECT DISTINCT 
    shippingid,
    shipping_plan_datetime,
    payment_amount,
    vendorid,
    shipping_country_id,
    transfer_type_id,
    (regexp_split_to_array(vendor_agreement_description, ':'))[1]::bigint AS agreementid
FROM public.shipping
LEFT JOIN public.shipping_country_rates USING (shipping_country)
LEFT JOIN public.shipping_transfer 
    ON shipping_transfer_description = concat_ws(':', transfer_type, transfer_model);

-- 5. Load shipping_status (Optimized using ROW_NUMBER and conditional aggregation milestones)
INSERT INTO public.shipping_status (
    shippingid,
    status,
    state,
    shipping_start_fact_datetime,
    shipping_end_fact_datetime
)
WITH ranked_states AS (
    SELECT 
        shippingid,
        status,
        state,
        ROW_NUMBER() OVER (
            PARTITION BY shippingid 
            ORDER BY state_datetime DESC, id DESC
        ) AS rn
    FROM public.shipping
),
milestones AS (
    SELECT 
        shippingid,
        MIN(state_datetime) FILTER (WHERE state = 'booked') AS shipping_start_fact_datetime,
        MAX(state_datetime) FILTER (WHERE state = 'recieved') AS shipping_end_fact_datetime
    FROM public.shipping
    GROUP BY shippingid
)
SELECT 
    r.shippingid,
    r.status,
    r.state,
    m.shipping_start_fact_datetime,
    m.shipping_end_fact_datetime
FROM ranked_states r
JOIN milestones m ON r.shippingid = m.shippingid
WHERE r.rn = 1;