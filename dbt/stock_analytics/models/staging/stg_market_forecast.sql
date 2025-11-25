{{ config(materialized='view') }}

with src as (
  select
    upper(symbol)                         as symbol,
    cast(date as date)                    as date,
    cast(actual as float)                 as actual,
    cast(forecast as float)               as forecast,
    cast(lower_bound as float)            as lower_bound,
    cast(upper_bound as float)            as upper_bound
  from {{ source('analytics','MARKET_DATA') }}
  where date is not null
),
future_only as (
  select
    symbol, date, forecast, lower_bound, upper_bound
  from src
  where actual is null
)
select distinct
  symbol,
  date,
  forecast,
  lower_bound,
  upper_bound,
  -- single-column pk for generic dbt `unique` test
  concat(symbol, '|', to_varchar(date)) as pk_symbol_date
from future_only