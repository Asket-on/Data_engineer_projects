DELETE FROM cdm.dm_courier_ledger;
with cte1 as (select 
    f.*,
    o.order_ts AS order_ts,
    d.rate as rate,
    c.courier_id AS courier_id,
    c.courier_name AS courier_name,
    EXTRACT(YEAR FROM order_ts) AS settlement_year,
    EXTRACT(MONTH FROM order_ts) AS settlement_month,
    avg(rate) over (partition by c.courier_id, 
	    EXTRACT(YEAR FROM order_ts), 
	    EXTRACT(MONTH FROM order_ts)) as rate_avg
from dds.fct_api_sales f
left join dds.dm_api_orders o
on f.order_id = o.order_id
left join dds.dm_api_delivery_details d
on f.delivery_id = d.delivery_id
left join dds.dm_api_couriers c
on d.courier_id = c.courier_id
),
cte2 as (select
	settlement_year, settlement_month, courier_id, courier_name, rate_avg, tip_sum, order_id,order_sum,
    case
    	when rate_avg < 4 and order_sum * 0.05 > 100
    	then order_sum * 0.05
    	when rate_avg < 4 and order_sum * 0.05 < 100
    	then 100
    	when rate_avg < 4.5 and order_sum * 0.07 > 150
    	then order_sum * 0.07
    	when rate_avg < 4.5 and order_sum * 0.07 < 150
    	then 150
    	when rate_avg < 4.9 and order_sum * 0.08 > 175
    	then order_sum * 0.08
    	when rate_avg < 4.9 and order_sum * 0.08 < 175
    	then 175
    	when rate_avg >= 4.9 and order_sum * 0.10 > 200
    	then order_sum * 0.10    
    	when rate_avg >= 4.9 and order_sum * 0.10 < 200
    	then 200  	
    end as courier_order_sum
from cte1)
INSERT INTO cdm.dm_courier_ledger (
    settlement_year,
    settlement_month,
    courier_id,
    courier_name,
    orders_count,
    orders_total_sum,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum,
    rate_avg
)
SELECT settlement_year, settlement_month, courier_id, courier_name, rate_avg,
count(*) as orders_count,
sum(order_sum) as orders_total_sum,
sum(order_sum) * 0.25 as order_processing_fee,
sum(courier_order_sum) as courier_order_sum,
sum(tip_sum) as courier_tips_sum,
sum(courier_order_sum) + sum(tip_sum) * 0.95 as courier_reward_sum
from cte2
group by (settlement_year, settlement_month, courier_id, courier_name, rate_avg);