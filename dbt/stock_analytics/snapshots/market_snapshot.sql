{% snapshot market_data_snapshot %}
{{
    config(
      target_database='USER_DB_OSTRICH',
      target_schema='SNAPSHOTS',
      unique_key="concat(symbol, '|', to_varchar(date))",
      strategy='check',
      check_cols=['CLOSE', 'VOLUME'],
    )
}}
select * from {{ source('raw', 'MARKET_DATA') }}
{% endsnapshot %}