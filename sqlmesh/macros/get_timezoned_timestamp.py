from sqlmesh import macro

@macro()
def get_timezoned_timestamp(evaluator, geo_code_col, datetime_col):
    """
    Retourne une expression SQL CASE avec timezone en fonction du geo_code.
    """
    return f"""
        CASE
          WHEN {geo_code_col}::varchar ~ '^97[1-2]'
            THEN {datetime_col} AT TIME ZONE 'America/Guadeloupe'
          WHEN {geo_code_col}::varchar ~ '^973'
            THEN {datetime_col} AT TIME ZONE 'America/Cayenne'
          WHEN {geo_code_col}::varchar ~ '^974'
            THEN {datetime_col} AT TIME ZONE 'Indian/Reunion'
          WHEN {geo_code_col}::varchar ~ '^976'
            THEN {datetime_col} AT TIME ZONE 'Indian/Mayotte'
          ELSE {datetime_col} AT TIME ZONE 'Europe/Paris'
        END
    """