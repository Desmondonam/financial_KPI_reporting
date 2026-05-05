{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}

with source as (

    select * from {{ source('raw', 'lending_club_loans') }}

),

cleaned as (

    select
        -- identifiers
        cast(id as varchar)           as loan_id,

        -- loan economics
        cast(loan_amnt   as numeric)  as loan_amount,
        cast(funded_amnt as numeric)  as funded_amount,
        trim(term)                    as term,
        cast(int_rate    as numeric)  as interest_rate,
        cast(installment as numeric)  as installment,

        -- borrower characteristics
        trim(grade)                   as grade,
        trim(sub_grade)               as sub_grade,
        trim(emp_length)              as emp_length,
        trim(home_ownership)          as home_ownership,
        cast(annual_inc as numeric)   as annual_income,
        trim(verification_status)     as verification_status,
        cast(dti as numeric)          as dti,
        cast(fico_range_low  as numeric) as fico_low,
        cast(fico_range_high as numeric) as fico_high,

        -- loan metadata
        trim(issue_d)                 as issue_date,
        trim(loan_status)             as loan_status,
        trim(purpose)                 as purpose,
        trim(addr_state)              as state,

        -- payment history
        cast(total_pymnt     as numeric) as total_payment,
        cast(total_rec_int   as numeric) as total_interest_received,
        cast(total_rec_prncp as numeric) as total_principal_received,
        cast(recoveries      as numeric) as recoveries,

        -- derived flag
        case
            when lower(trim(loan_status)) in (
                'default', 'charged off',
                'does not meet the credit policy. status:charged off'
            )
            then 1 else 0
        end                           as is_default

    from source
    where loan_amnt is not null

)

select * from cleaned
