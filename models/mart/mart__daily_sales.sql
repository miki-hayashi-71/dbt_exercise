with cleansed_orders as (select * from {{ ref("int__cleansed_orders") }})

select
    date(order_time_jst) as order_date,
    count(distinct order_id) as total_orders,
    sum(sales_jpy) as total_sales
from cleansed_orders
group by order_date
order by order_date
