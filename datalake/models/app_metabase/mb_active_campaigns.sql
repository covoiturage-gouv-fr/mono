{{ config(
  materialized='view',
  tags=['app_metabase', 'active_campaigns']
) }}

with list as (
  select
    p._id,
    p.name,
    p.territory_name as territory,
    p.start_date,
    p.end_date,
    p.max_amount     as enveloppe,
    pis.validated,
    pis.draft
  from {{ ref('policies') }} as p
  left join
    {{ ref('policy_incentives_summary') }}
      as pis
    on p._id = pis.campaign_id
  where
    p.status = 'active'
    and p.end_date > now()
)

select
  _id,
  territory,
  start_date,
  end_date,
  validated,
  draft,
  enveloppe,
  name,
  (
    validated + draft
  ) as total_encours,
  (
    date_part('day', now() - start_date)
    / date_part('day', end_date - start_date)
  ) as conso_jours
from list
order by validated desc
