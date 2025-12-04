{{
    config(
        alias="daily_registered_user_types",
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
    orders as (select user_id, created_at from {{ ref("stg__orders") }}),
    events as (
        select user_id, created_at
        from {{ ref("stg__events") }}
        where user_id is not null
    ),

    -- ordersのユーザーのログとeventsのユーザーのログを結合。各ユーザーのログを日付単位で表示（同一ユーザーが同日に複数ログがあった場合は重複を排除）
    daily_access as (
        select user_id, date(created_at, "+9") as date
        from orders

        union distinct

        select user_id, date(created_at, "+9")
        from events
    ),

    -- 各ユーザーの日付ごとのログに対して、前回アクセス日と翌回アクセス日を取得
    with_prev_next_date as (
        select
            user_id,
            date,
            lag(date) over (partition by user_id order by date) as last_access_date,
            lead(date) over (partition by user_id order by date) as next_access_date
        from daily_access
    )

select
    user_id,
    date,

    -- ユーザータイプの判定
    case
        when last_access_date is null
        then "新規"
        when date_diff(date, last_access_date, day) > 14
        then "復帰"
        else "既存"
    end as user_type,

    -- 次のアクセスまでの期間を判定しフラグを立てる
    case
        when date_diff(next_access_date, date, day) = 1 then 1 else 0
    end as d1_access_flg,

    case
        when date_diff(next_access_date, date, day) between 1 and 3 then 1 else 0
    end as d1_3_access_flg,

    case
        when date_diff(next_access_date, date, day) between 1 and 7 then 1 else 0
    end as d1_7_access_flg,

    case
        when date_diff(next_access_date, date, day) between 1 and 14 then 1 else 0
    end as d1_14_access_flg

from with_prev_next_date
