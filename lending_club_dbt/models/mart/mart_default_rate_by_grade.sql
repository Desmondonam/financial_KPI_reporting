{{
  config(
    materialized = 'table',
    schema       = 'mart'
  )
}}

with base as (

    select
        grade,
        is_default,
        loan_amount
    from {{ ref('stg_loans') }}
    where grade is not null

),

aggregated as (

    select
        grade,
        count(*)                                             as total_loans,
        sum(is_default)                                      as defaulted_loans,
        count(*) - sum(is_default)                           as performing_loans,
        round(
            cast(sum(is_default) as numeric)
            / nullif(cast(count(*) as numeric), 0) * 100,
            2
        )                                                    as default_rate_pct,
        round(avg(loan_amount), 2)                           as avg_loan_amount_usd,
        sum(loan_amount)                                     as total_loan_volume_usd
    from base
    group by grade

)

select * from aggregated
order by grade
