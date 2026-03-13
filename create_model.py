#!/usr/bin/env python3

import copy
import os
import sys
import json
import requests

script_dir = os.path.dirname(os.path.abspath(__file__))
sigma_model_dir = os.path.join(script_dir, "sigma_model")


def _path_to_sql(path):
    """Build a SELECT * FROM quoted identifier from a warehouse path (e.g. [db, schema, table])."""
    if not path or not isinstance(path, list):
        return None
    # BigQuery / generic: use backticks for project.dataset.table or schema.table
    parts = [str(p).strip().strip("`\"'") for p in path if p]
    if not parts:
        return None
    if len(parts) == 1:
        return f"SELECT * FROM \"{parts[0]}\""
    # Multi-part: backticks for project.dataset.table (BigQuery) or "schema"."table" (others)
    qualified = ".".join(parts)
    return f"SELECT * FROM `{qualified}`"


def _should_retry_with_custom_sql(response):
    """True if create failed in a way that retrying with custom SQL sources might fix (e.g. table not in UI)."""
    if response is None or response.status_code < 400:
        return False
    # Retry on any 400/422 when creating a data model; often table path isn't available in UI.
    if response.status_code not in (400, 422):
        return False
    text = getattr(response, "text", None) or ""
    if response.headers.get("content-type", "").startswith("application/json") and text:
        try:
            text += " " + json.dumps(response.json())
        except Exception:
            pass
    text_lower = text.lower()
    # Explicit table/UI messages, or any validation-like error (table references often fail validation).
    return (
        "table" in text_lower
        or "no ui option" in text_lower
        or "no option for this table" in text_lower
        or "path" in text_lower
        or "element" in text_lower
        or "source" in text_lower
    )


def _log_sigma_error(response):
    """Log full Sigma API error (status + body) so it can be used to align custom SQL schema."""
    if response is None:
        return
    print(f"  Sigma API status: {response.status_code}")
    body = getattr(response, "text", None) or ""
    if body:
        print(f"  Sigma API response (full): {body}")


def _payload_warehouse_to_custom_sql(payload):
    """Return a deep copy of the payload with every warehouse-table element converted to Sigma custom SQL.
    Sigma API: kind "sql", SQL in "statement"; column formulas must reference [Custom SQL/col_id]."""
    out = copy.deepcopy(payload)
    for page in out.get("pages") or []:
        for el in page.get("elements") or []:
            src = el.get("source") or {}
            if src.get("kind") != "warehouse-table":
                continue
            path = src.get("path")
            stmt = _path_to_sql(path)
            if not stmt:
                continue
            conn = src.get("connectionId")
            el["source"] = {
                "connectionId": conn,
                "kind": "sql",
                "statement": stmt,
            }
            # Sigma custom SQL elements use [Custom SQL/column_id] in formulas
            elem_id = el.get("id", "")
            for col in el.get("columns") or []:
                if "formula" in col and elem_id:
                    col["formula"] = col["formula"].replace(f"[{elem_id}/", "[Custom SQL/")
            for m in el.get("metrics") or []:
                if "formula" in m and elem_id:
                    m["formula"] = m["formula"].replace(f"[{elem_id}/", "[Custom SQL/")
            if "order" not in el and el.get("columns"):
                el["order"] = [c["id"] for c in el["columns"]]
    return out

# 1. Load credentials from environment variables
client_id = os.getenv("SIGMA_CLIENT_ID")
client_secret = os.getenv("SIGMA_CLIENT_SECRET")
api_base_url = os.getenv("SIGMA_API_BASE_URL", "https://api.sigmacomputing.com")

if not client_id or not client_secret:
    print("Error: Missing SIGMA_CLIENT_ID or SIGMA_CLIENT_SECRET environment variables.")
    sys.exit(1)

# 2. Authenticate once
print("Authenticating with Sigma API...")
auth_url = f"{api_base_url}/v2/auth/token"
auth_payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
}
auth_headers = {"Content-Type": "application/x-www-form-urlencoded"}

auth_response = None
try:
    auth_response = requests.post(auth_url, data=auth_payload, headers=auth_headers)
    auth_response.raise_for_status()
    access_token = auth_response.json().get("access_token")
    print("Authentication successful!")
except requests.exceptions.RequestException as e:
    print(f"Authentication failed: {e}")
    if auth_response is not None and getattr(auth_response, "text", None):
        print(auth_response.text)
    sys.exit(1)

# 3. Find all JSON files under sigma_model/ (any depth)
json_files = []
if os.path.isdir(sigma_model_dir):
    for root, _dirs, files in os.walk(sigma_model_dir):
        for f in files:
            if f.lower().endswith(".json"):
                json_files.append(os.path.join(root, f))
else:
    print(f"Error: sigma_model directory not found: {sigma_model_dir}")
    sys.exit(1)

if not json_files:
    print(f"No JSON files found under {sigma_model_dir}")
    sys.exit(0)

print(f"\nFound {len(json_files)} JSON file(s):")
for path in sorted(json_files):
    print(f"  - {os.path.relpath(path, script_dir)}")
print("\nCreating data model(s) in Sigma...")
create_dm_url = f"{api_base_url}/v2/dataModels/spec"
api_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

failed = 0
created = 0
for path in sorted(json_files):
    rel_path = os.path.relpath(path, script_dir)
    print(f"\n--- {rel_path} ---")
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except json.JSONDecodeError as e:
        print(f"  Error: invalid JSON: {e}")
        failed += 1
        continue

    name = payload.get("name", os.path.basename(path))
    response = None
    try:
        response = requests.post(create_dm_url, headers=api_headers, json=payload)
        response.raise_for_status()
        created += 1
        body = response.json()
        print(f"  Created data model: {name}")
        if body.get("id"):
            print(f"  Data model ID: {body.get('id')}")
        if os.environ.get("CREATE_MODEL_VERBOSE"):
            print(f"  Response: {json.dumps(body, indent=2)}")
    except requests.exceptions.RequestException as e:
        response = getattr(e, "response", None)
        if response is not None and _should_retry_with_custom_sql(response):
            try:
                payload_custom = _payload_warehouse_to_custom_sql(payload)
                retry_resp = requests.post(create_dm_url, headers=api_headers, json=payload_custom)
                retry_resp.raise_for_status()
                created += 1
                body = retry_resp.json()
                print(f"  Created data model: {name} (using custom SQL)")
                if body.get("id"):
                    print(f"  Data model ID: {body.get('id')}")
                if os.environ.get("CREATE_MODEL_VERBOSE"):
                    print(f"  Response: {json.dumps(body, indent=2)}")
            except requests.exceptions.RequestException as retry_e:
                print(f"  Failed to create data model '{name}': {retry_e}")
                _log_sigma_error(retry_e.response)
                failed += 1
        else:
            print(f"  Failed to create data model '{name}': {e}")
            _log_sigma_error(response)
            failed += 1

if failed:
    print(f"\n{failed} of {len(json_files)} model(s) failed; {created} created.")
    sys.exit(1)
print(f"\nAll {len(json_files)} model(s) created successfully ({created} total).")
