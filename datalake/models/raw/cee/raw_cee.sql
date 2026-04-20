{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['_id'],
    indexes = [
      { 'columns':['carpool_v2_id'] },
    ],
    tags=['raw', 'cee']
) }}

SELECT
  cee._id,
  COALESCE(cv2_id._id, cv2_journey._id) AS carpool_v2_id,
  cee.operator_id,
  cee.operator_journey_id,
  cee.datetime,
  cee.journey_type::VARCHAR AS journey_type,
  cee.is_specific,
  cee.application_timestamp,
  cee.created_at,
  cee.updated_at
FROM cee.cee_applications cee
LEFT JOIN {{ source('carpool_v1', 'carpools') }} cv1
  ON cee.carpool_id IS NOT NULL AND cv1._id = cee.carpool_id
LEFT JOIN {{ source('carpool_v2', 'carpools') }} cv2_id
  ON cv1._id IS NOT NULL AND cv2_id.legacy_id = cv1.acquisition_id
LEFT JOIN {{ source('carpool_v2', 'carpools') }} cv2_journey
  ON cee.carpool_id IS NULL
  AND cv2_journey.operator_id = cee.operator_id
  AND cv2_journey.operator_journey_id = cee.operator_journey_id
WHERE {{ time_filter('cee.datetime', 'datetime') }}