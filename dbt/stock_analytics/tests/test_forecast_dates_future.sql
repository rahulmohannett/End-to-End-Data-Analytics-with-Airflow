-- Fail if any forecast date is not strictly after the latest historical date for that symbol.
with max_hist as (
  select symbol, max(date) as max_hist_date
  from {{ ref('mart_stock_analysis') }}
  where record_type = 'historical'
  group by 1
)

select f.symbol, f.date, f.record_type, f.forecast_price, m.max_hist_date
from {{ ref('mart_stock_analysis') }} f
join max_hist m using (symbol)
where f.record_type = 'forecast'
  and f.date <= m.max_hist_date