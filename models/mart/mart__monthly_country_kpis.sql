{{
    config(
        alias="monthly_country_kpis",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "month",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    daily_user_sales as (
        select *
        from {{ ref("int__daily_user_sales") }}
        {% if is_incremental() %}
            where date >= date_trunc(current_date("+9") - 30, month)
        {% endif %}
    ),

    -- 月次のユーザーごとの売上を集計し、国セグメントを付与
    monthly_user_sales_with_country_segment as (
        select
            date_trunc(date, month) as month,
            case
                when country = "Japan"
                then "国内"
                when country = "United States"
                then "US"
                else "その他海外"
            end as country_segment,
            user_id,
            sum(sales) as monthly_user_sales
        from daily_user_sales
        group by 1, 2, 3
    ),

    -- メタ情報に沿って出力
    final as (
        select
            month,
            country_segment,
            count(distinct user_id) as mau,
            sum(monthly_user_sales) as sales,
            count(
                distinct case when monthly_user_sales > 0 then user_id end
            ) as payment_uu,
            safe_divide(
                sum(monthly_user_sales),
                count(distinct case when monthly_user_sales > 0 then user_id end)
            ) as arppu,

            approx_quantiles(
                case when monthly_user_sales > 0 then monthly_user_sales end, 4
            )[offset(1)] as payment_sales_q1,
            approx_quantiles(
                case when monthly_user_sales > 0 then monthly_user_sales end, 4
            )[offset(2)] as payment_sales_q2,
            approx_quantiles(
                case when monthly_user_sales > 0 then monthly_user_sales end, 4
            )[offset(3)] as payment_sales_q3

        from monthly_user_sales_with_country_segment
        group by 1, 2
    )

select *
from final
