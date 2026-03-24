import json
from datetime import date, datetime

import pandas as pd


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime().isoformat()
    if pd.isna(value):
        return None
    return str(value)


def serialize_result(result):
    if isinstance(result, pd.DataFrame):
        normalized = result.where(pd.notna(result), None)
        payload = {
            "columns": list(normalized.columns),
            "records": normalized.to_dict(orient="records"),
        }
        return "dataframe", json.dumps(payload, ensure_ascii=False, default=json_default)

    if result is None:
        return "null", "null"

    payload = json.dumps(result, ensure_ascii=False, default=json_default)
    if isinstance(result, (dict, list, tuple)):
        return "json", payload
    return "primitive", payload


def deserialize_result(result_type, result_json):
    if result_type == "dataframe":
        payload = json.loads(result_json or "{}")
        return pd.DataFrame(payload.get("records") or [], columns=payload.get("columns") or None)
    if result_type == "null":
        return None
    if result_type in {"json", "primitive"}:
        return json.loads(result_json or "null")
    return json.loads(result_json or "null")
