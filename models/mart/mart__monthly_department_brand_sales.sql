{{
    config(
        alias="monthly_department_brand_sales",
        materialized="table",
        partition_by={"field": "month", "data_type": "date", "granularity": "month"},
    )
}}

with
    cleansed_orders as (select * from {{ ref("int__cleansed_orders") }}),

    monthly_registered_user_types as (
        select * from {{ ref("int__monthly_registered_user_types") }}
    ),

    -- 月・部門・ブランドごとのuuを集計
    cleansed_orders_with_monthly_payment_uu as (
        select
            sales_jpy,
            user_id,
            product_department,
            product_brand,
            date(date_trunc(order_time_jst, month)) as month,
            count(distinct user_id) over (
                partition by
                    date(date_trunc(order_time_jst, month)),
                    product_department,
                    product_brand
            ) as brand_total_uu
        from cleansed_orders
    ),

    -- 月・部門・ブランド毎のuuが10人未満の場合、ブランド名をothersに置き換える
    final as (
        select
            month,
            user_type,
            product_department as department,
            case when brand_total_uu < 10 then "Others" else product_brand end as brand,
            sum(sales_jpy) as sales,
            count(distinct user_id) as payment_uu
        from cleansed_orders_with_monthly_payment_uu
        left join monthly_registered_user_types using (user_id, month)
        group by 1, 2, 3, 4
    )

select *
from final
