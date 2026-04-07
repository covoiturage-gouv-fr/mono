"""Timezone resolution macro for overseas territories.

Converts a UTC timestamp to local time based on the commune geo_code.
Handles Guadeloupe, Martinique, Guyane, Reunion, Mayotte; defaults to Europe/Paris.
"""

from sqlmesh import macro


@macro()
def get_timezoned_timestamp(evaluator, geo_code_col, datetime_col):
    geo = geo_code_col.sql(dialect="postgres")
    dt = datetime_col.sql(dialect="postgres")
    return f"""CASE
    WHEN {geo}::VARCHAR ~ '^97[1-2]'
      THEN {dt} AT TIME ZONE 'America/Guadeloupe'
    WHEN {geo}::VARCHAR ~ '^973'
      THEN {dt} AT TIME ZONE 'America/Cayenne'
    WHEN {geo}::VARCHAR ~ '^974'
      THEN {dt} AT TIME ZONE 'Indian/Reunion'
    WHEN {geo}::VARCHAR ~ '^976'
      THEN {dt} AT TIME ZONE 'Indian/Mayotte'
    ELSE {dt} AT TIME ZONE 'Europe/Paris'
  END"""
