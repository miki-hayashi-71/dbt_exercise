{{
    config(
        alias="daily_user_sales",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["user_id"],
    )
}}

with
    cleansed_orders as (select * from {{ ref("int__cleansed_orders") }}),

    -- ユーザーごとの日次の売上
    daily_user_sales as (
        select
            user_id,
            date(date_trunc(order_time_jst, day)) as date,
            sum(sales_jpy) as sales
        from cleansed_orders
        group by 1, 2
    ),

    -- 過去の売上の集計
    calc_past_sales as (
        select
            user_id,
            date,
            sales,
            -- 30日前〜昨日購入までの累計
            sum(sales) over (
                partition by user_id
                order by unix_date(date)
                range between 30 preceding and 1 preceding
            ) as past_d30_sales,
            -- 初回〜昨日購入までの累計
            sum(sales) over (
                partition by user_id
                order by unix_date(date)
                range between unbounded preceding and 1 preceding
            ) as past_all_sales
        from daily_user_sales
    ),

    -- メタ情報に合わせて出力
    final as (
        select
            user_id,
            date,
            sales,
            coalesce(past_d30_sales, 0) as past_d30_sales,
            coalesce(past_all_sales, 0) as past_all_sales,
            case
                when coalesce(past_d30_sales, 0) >= 50001
                then "a_50,001円~"
                when coalesce(past_d30_sales, 0) >= 30001
                then "b_30,001円~50,000円"
                when coalesce(past_d30_sales, 0) >= 10001
                then "c_10,001円~30,000円"
                when coalesce(past_d30_sales, 0) >= 5001
                then "d_5,001円~10,000円"
                when coalesce(past_d30_sales, 0) >= 3001
                then "e_3,001円~5,000円"
                when coalesce(past_d30_sales, 0) >= 1001
                then "f_1,001円~3,000円"
                when coalesce(past_d30_sales, 0) >= 1
                then "g_1円~1,000円,"
                else "h_0円"
            end as past_d30_payment_segment,
            case
                when coalesce(past_all_sales, 0) > 0 then 1 else 0
            end as payment_experience_flg
        from calc_past_sales
    )

select *
from final
