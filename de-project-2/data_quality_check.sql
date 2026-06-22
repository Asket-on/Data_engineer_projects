-- Check 1: Null Check (Should return 0)
SELECT COUNT(*) 
FROM public.shipping_datamart 
WHERE shippingid IS NULL 
   OR vendorid IS NULL 
   OR payment_amount IS NULL 
   OR vat IS NULL 
   OR profit IS NULL;

-- Check 2: Range Check (Should return 0)
SELECT COUNT(*) 
FROM public.shipping_datamart 
WHERE full_day_at_shipping < 0 
   OR delay_day_at_shipping < 0 
   OR payment_amount < 0 
   OR vat < 0 
   OR profit < 0;

-- Check 3: Consistency Check (Should return 0)
SELECT COUNT(*) 
FROM public.shipping_datamart dm
JOIN public.shipping_info si USING (shippingid)
JOIN public.shipping_country_rates cr USING (shipping_country_id)
JOIN public.shipping_transfer st USING (transfer_type_id)
JOIN public.shipping_agreement sa USING (agreementid)
WHERE ABS(dm.vat - (dm.payment_amount * (cr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate))) > 0.01
   OR ABS(dm.profit - (dm.payment_amount * sa.agreement_commission)) > 0.01;

-- Check 4: Completeness Check (Should return 0)
SELECT ABS(
    (SELECT COUNT(DISTINCT shippingid) FROM public.shipping_datamart) - 
    (SELECT COUNT(DISTINCT shippingid) FROM public.shipping)
);
