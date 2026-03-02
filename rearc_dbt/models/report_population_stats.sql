select
  avg(population) as mean_population,
  stddev_samp(population) as stddev_population
from {{ source('rearc_schema', 'stg_population_raw') }}
where year between 2013 and 2018
  and nation = 'United States'
