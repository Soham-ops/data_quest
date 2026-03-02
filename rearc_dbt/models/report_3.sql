select
    b.series_id,
    b.year,
    b.period,
    b.value,
    p.population
from {{ source('rearc_schema', 'stg_bls_pr_data_current') }} b
left join {{ source('rearc_schema', 'stg_population_raw') }} p
  on b.year = p.year
 and p.nation = 'United States'
where b.series_id = 'PRS30006032'
  and b.period = 'Q01'
order by b.year
