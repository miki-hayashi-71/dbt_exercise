{{
    config(
        alias="monthly_department_brand_sales",
        materialized="table",
        partition_by={"field": "month", "data_type": "date", "granularity": "month"},
    )
}}

with
    cleansed_orders as (select * from {{ ref("int__cleansed_orders") }}),
    monthly_user_types as (
        select * from {{ ref("int__monthly_registered_user_types") }}
    ),

    -- 月次の部門・ブランドごとのユニークなユーザーを集計
    monthly_brands_uu as (
        select
            date(date_trunc(order_time_jst, month)) as month,
            product_department,
            product_brand,
            sum(sales_jpy) as sum_sales,
            count(distinct user_id) as payment_uu
        from cleansed_orders
        group by 1, 2, 3
    ),

    -- monthly_brands_uuが10未満のブランドをOthersに変換し、cleansed_ordersのbrandと置き換える
    cleansed_orders_with_others as (
        select
            date(date_trunc(cleansed_orders.order_time_jst, month)) as month,
            cleansed_orders.product_department,
            case
                when monthly_brands_uu.payment_uu < 10
                then "Others"
                else monthly_brands_uu.product_brand
            end as product_brand,
            user_id,
            sales_jpy,
        from cleansed_orders
        left join
            monthly_brands_uu
            on date(date_trunc(cleansed_orders.order_time_jst, month))
            = monthly_brands_uu.month
            and cleansed_orders.product_department
            = monthly_brands_uu.product_department
            and cleansed_orders.product_brand = monthly_brands_uu.product_brand
    ),

    -- 月、ユーザータイプ、部門、ブランド名でグルーピングして出力
    final as (
        select
            month,
            user_type,
            product_department as department,
            product_brand as brand,
            sum(sales_jpy) as sales,
            count(distinct user_id) as payment_uu
        from cleansed_orders_with_others
        left join monthly_registered_user_types using (user_id, month)
        group by 1, 2, 3, 4
    )

select *
from final
