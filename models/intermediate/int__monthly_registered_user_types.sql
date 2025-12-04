{{
    config(
        alias="monthly_registered_user_types",
        materialized="table",
        partition_by={
            "field": "month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["user_id"],
    )
}}

with
    daily_registered_user_types as (
        select * from {{ ref("int__daily_registered_user_types") }}
    ),

    final as (
        select
            date_trunc(date, month) as month,
            user_id,
            case
                when countif(user_type = "新規") > 0
                then "新規"
                when countif(user_type = "復帰") > 0
                then "復帰"
                else "既存"
            end as user_type
        from daily_registered_user_types
        group by 1, 2
    )

select *
from final
