{{
    config(
        materialized="table",
        partition_by={
            "field": "order_time_jst",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["user_id"],
    )
}}


with
    orders as (select * from {{ ref("stg__orders") }}),

    order_items as (select * from {{ ref("stg__order_items") }})

select
    orders.order_id,
    orders.user_id,
    datetime_add(orders.created_at, interval 9 hour) as order_time_jst,
    order_items.product_id,
    order_items.inventory_item_id,
    order_items.sale_price * 150 as sales_jpy
from orders
left join order_items on order_items.order_id = orders.order_id
where orders.returned_at is null
