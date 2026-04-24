{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['territory_1', 'territory_2','carpool_date'],
    indexes = [
      { 'columns':['territory_1', 'territory_2','carpool_date'], 'unique': true },
    ],
    tags=['refined', 'od', 'day_arr']
) }}