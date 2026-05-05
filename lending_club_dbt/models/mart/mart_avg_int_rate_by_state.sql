{{
  config(
    materialized = 'table',
    schema       = 'mart'
  )
}}

with base as (

    select
        state,
        interest_rate,
        loan_amount,
        is_default
    from {{ ref('stg_loans') }}
    where state is not null

),

aggregated as (

    select
        state,
        count(*)                             as loan_count,
        round(avg(interest_rate), 2)         as avg_interest_rate_pct,
        round(min(interest_rate), 2)         as min_interest_rate_pct,
        round(max(interest_rate), 2)         as max_interest_rate_pct,
        sum(loan_amount)                     as total_loan_volume_usd,
        round(
            cast(sum(is_default) as numeric)
            / nullif(cast(count(*) as numeric), 0) * 100,
            2
        )                                    as default_rate_pct
    from base
    group by state

)

select * from aggregated
order by loan_count desc
