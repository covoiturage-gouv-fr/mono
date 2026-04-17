import json

def load_config(path_or_json: str) -> list:
    if path_or_json.strip().startswith("["):
        return json.loads(path_or_json)
    with open(path_or_json) as f:
        return json.load(f)