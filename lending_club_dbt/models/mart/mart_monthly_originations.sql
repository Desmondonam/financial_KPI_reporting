{{
  config(
    materialized = 'table',
    schema       = 'mart'
  )
}}

with base as (

    select
        issue_date,
        loan_amount
    from {{ ref('stg_loans') }}
    where issue_date is not null

),

aggregated as (

    select
        issue_date                   as month,
        count(*)                     as loan_count,
        sum(loan_amount)             as total_originated_usd,
        avg(loan_amount)             as avg_loan_amount_usd,
        min(loan_amount)             as min_loan_amount_usd,
        max(loan_amount)             as max_loan_amount_usd
    from base
    group by issue_date

)

select * from aggregated
order by month
