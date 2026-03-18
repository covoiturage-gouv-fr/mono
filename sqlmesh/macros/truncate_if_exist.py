from sqlmesh import macro

@macro()
def truncate_if_exist(evaluator, table):
    """
    @truncate_if_exist(@this_model)
    """
    if evaluator.runtime_stage == "creating":
        return None
    table_str = str(table)
    return (
      f"TRUNCATE TABLE {table_str}"
    )