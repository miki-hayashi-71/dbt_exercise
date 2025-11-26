


with
    orders as (select * from `data-bootcamp-477503`.`dbt_hayashi`.`stg__orders`),

    order_items as (select * from `data-bootcamp-477503`.`dbt_hayashi`.`stg__order_items`)

select
    orders.order_id,
    orders.user_id,
    datetime_add(orders.created_at, interval 9 hour) as order_time_jst,
    order_items.product_id,
    order_items.inventory_item_id,
    order_items.sale_price * 150 as sales_jpy
from orders
left join order_items on orders.order_id = order_items.order_id
where
    order_items.status not in ("Cancelled", "Returned")
    or orders.status not in ("Cancelled", "Returned")