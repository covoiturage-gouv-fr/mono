from sqlmesh import macro

FINAL_ACQUISITION_STATUSES = [
    'processed',
    'failed',
    'canceled',
    'expired',
    'terms_violation_error',
]

@macro()
def get_final_acquisition_status_list(evaluator):
    # Retourne une clause SQL valide
    return "(" + ", ".join([f"'{s}'" for s in FINAL_ACQUISITION_STATUSES]) + ")"