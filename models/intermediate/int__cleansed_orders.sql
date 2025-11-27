{{
    config(
        materialized="table",
        partition_by={
            "field": "order_time_jst",
            "data_type": "datetime",
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
    datetime(orders.created_at, "+9") as order_time_jst,
    order_items.product_id,
    order_items.inventory_item_id,
    order_items.sale_price * 150 as sales_jpy
from orders
left join order_items on orders.order_id = order_items.order_id
where
    order_items.status not in ("Cancelled", "Returned")
    or orders.status not in ("Cancelled", "Returned")
