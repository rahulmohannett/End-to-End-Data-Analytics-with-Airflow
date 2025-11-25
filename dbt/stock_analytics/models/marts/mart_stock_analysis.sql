{{ config(materialized='table') }}

-- Historical slice
with hist as (
  select
    symbol,
    date,
    close                  as actual_price,
    cast(null as float)    as forecast_price,
    cast(null as float)    as lower_bound,
    cast(null as float)    as upper_bound,
    'historical'           as record_type,
    concat(symbol, '|', to_varchar(date)) as pk_symbol_date
  from {{ ref('stg_market_data') }}
),

-- Forecast slice
fut as (
  select
    symbol,
    date,
    cast(null as float)    as actual_price,
    forecast               as forecast_price,
    lower_bound,
    upper_bound,
    'forecast'             as record_type,
    concat(symbol, '|', to_varchar(date)) as pk_symbol_date
  from {{ ref('stg_market_forecast') }}
),

unioned as (
  select * from hist
  union all
  select * from fut
)

select *
from (
  select
    *,
    row_number() over (
      -- 1. PARTITION ONLY BY KEY (remove record_type)
      partition by symbol, date
      -- 2. PRIORITIZE HISTORICAL DATA
      order by 
        case when record_type = 'historical' then 1 else 2 end asc,
        date desc
    ) as rn
  from unioned
)
where rn = 1