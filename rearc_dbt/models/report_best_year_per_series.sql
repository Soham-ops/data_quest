with yearly as (
    select
        series_id,
        year,
        sum(value) as year_value
    from {{ source('rearc_schema', 'stg_bls_pr_data_current') }}
    where period in ('Q01', 'Q02', 'Q03', 'Q04')
      and year is not null
      and value is not null
    group by 1, 2
),
ranked as (
    select
        series_id,
        year,
        year_value,
        row_number() over (
            partition by series_id
            order by year_value desc, year desc
        ) as rn
    from yearly
)
select
    series_id,
    year,
    year_value as value
from ranked
where rn = 1
