{{
    config(
        alias="daily_kpis",
        materialized="table",
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    daily_user_sales as (select * from {{ ref("int__daily_user_sales") }}),

    daily_registered_user_types as (
        select * from {{ ref("int__daily_registered_user_types") }}
    ),

    joined_daily_user_sales_with_user_types as (
        select
            user_id,
            date,
            case
                when user_type = "新規"
                then "新規"
                when user_type = "復帰" and payment_experience_flg = 1
                then "復帰課金経験"
                when user_type = "復帰" and payment_experience_flg = 0
                then "復帰無課金"
                when user_type = "既存" and payment_experience_flg = 1
                then "既存課金景観"
                when user_type = "既存" and payment_experience_flg = 0
                then "既存無課金"
            end as detail_user_type,
            d1_access_flg,
            d1_3_access_flg,
            d1_7_access_flg,
            d1_14_access_flg,
            sales,
            payment_experience_flg
        from daily_registered_user_types
        left join daily_user_sales using (user_id, date)
    ),
    final as (
        select
            date,
            detail_user_type,
            count(distinct user_id) as dau,
            count(
                distinct case when detail_user_type = "新規" then user_id end
            ) as new_uu,
            sum(d1_access_flg) as d1_access_uu,
            sum(d1_3_access_flg) as d1_3_access_uu,
            sum(d1_7_access_flg) as d1_7_access_uu,
            sum(d1_14_access_flg) as d1_14_access_uu,
            count(distinct(case when sales > 0 then user_id end)) as payment_uu,
            sum(sales) as sales
        from joined_daily_user_sales_with_user_types
        group by date, detail_user_type
    )

select *
from final
