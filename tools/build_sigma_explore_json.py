#!/usr/bin/env python3
import sys
import glob
import json
import os
import re
import lkml
import uuid

def make_id(): return str(uuid.uuid4())[:10]

def extract_simple_join(sql_on):
    if not sql_on: return None
    match = re.match(r'^\s*\$\{([^.]+)\.([^}]+)\}\s*=\s*\$\{([^.]+)\.([^}]+)\}\s*$', sql_on)
    return match.groups() if match else None

def convert_formula(lookml_sql, physical_table, field_name, field_type=None, known_metrics=None):
    """Converts LookML syntax to Sigma Data Model bracket syntax, checking for derived metrics."""
    if known_metrics is None:
        known_metrics = set()
        
    if not lookml_sql:
        if field_type in ['count', 'count_distinct', 'location']:
            return "" # Counts are wrapped later, locations safely skipped
        return f"[{physical_table}/{field_name}]"
    
    # 1. Replace ${TABLE}."Column Name" with [TABLE_NAME/Column Name]
    s = re.sub(r'\$\{TABLE\}\."([^"]+)"', rf'[{physical_table}/\1]', lookml_sql)
    
    # 2. Replace ${TABLE}.column_name with [TABLE_NAME/column_name]
    s = re.sub(r'\$\{TABLE\}\.([a-zA-Z0-9_]+)', rf'[{physical_table}/\1]', s)
    
    # 3. Replace generic ${field_name} dynamically checking if it's a metric or dimension
    def replacer(match):
        ref = match.group(1)
        # Strip view name scoping if present (e.g. view_name.field_name -> field_name)
        clean_ref = ref.split('.')[-1] if '.' in ref else ref
        
        # Determine if it's referencing a known metric to apply [Metrics/...]
        if clean_ref in known_metrics:
            return f"[Metrics/{clean_ref}]"
        else:
            return f"[{clean_ref}]"
            
    s = re.sub(r'\$\{([^}]+)\}', replacer, s)
    
    return s

def main():
    lookml_dir = sys.argv[1] if len(sys.argv) > 1 else "lookml"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "sigma_model"
    model_name = (sys.argv[3] if len(sys.argv) > 3 else "").strip() or os.environ.get("CURRENT_MODEL_NAME", "").strip()
    model_key = (model_name or "").upper().replace("-", "_").replace(" ", "_")
    default_conn = "bee6615c-7d11-435c-8819-e32207b27fe4"
    env_conn = os.environ.get("CONNECTION_ID_" + model_key) or os.environ.get("CONNECTION_ID_DEFAULT", default_conn) if model_key else os.environ.get("CONNECTION_ID_DEFAULT", default_conn)
    env_db = os.environ.get("MANIFEST_DATABASE", "RETAIL")
    env_schema = os.environ.get("MANIFEST_SCHEMA", "PLUGS_ELECTRONICS")
    env_folder = os.environ.get("SIGMA_FOLDER_ID", "23xVmjTE4gZP7P6Wnpi4rA")

    views = {}
    known_metrics = set()
    model_explores = {}

    # 1. Discover .lkml; collect views, group explores by source model file
    seen_paths = set()
    for pattern in (lookml_dir + "/views/*.lkml", lookml_dir + "/models/*.lkml", lookml_dir + "/**/*.lkml"):
        for filepath in glob.glob(pattern, recursive=True):
            if filepath in seen_paths: continue
            seen_paths.add(filepath)
            try:
                with open(filepath, 'r') as f: parsed = lkml.load(f)
                if not parsed: continue
                for view in parsed.get("views", []):
                    v_name = view.get("name") if view else None
                    if v_name:
                        views[v_name] = view
                        for meas in (view.get("measures") or []):
                            if meas and meas.get("name"): known_metrics.add(meas["name"])
                explores_in_file = parsed.get("explores", [])
                if explores_in_file:
                    base = os.path.basename(filepath)
                    model_name_from_file = base.replace(".model.lkml", "").replace(".lkml", "")
                    if model_name_from_file not in model_explores: model_explores[model_name_from_file] = {}
                    for explore in explores_in_file:
                        e_name = explore.get("name") if explore else None
                        if e_name: model_explores[model_name_from_file][e_name] = explore
            except Exception: pass

    os.makedirs(output_dir, exist_ok=True)

    # 2. One Sigma JSON per LookML model file; remove stale JSONs from previous runs
    written_basenames = set()
    for model_name_from_file, explores in model_explores.items():
        elements = {}

        def add_element(v_name):
            if v_name not in elements:
                view_def = views.get(v_name, {})
                physical_table = v_name
                derived = view_def.get("derived_table")
                if isinstance(derived, dict) and derived.get("sql"):
                    raw_sql = (derived.get("sql") or "").strip().rstrip(";")
                    physical_table = "Custom SQL"
                    source = {"connectionId": env_conn, "kind": "sql", "statement": raw_sql}
                else:
                    table_name = view_def.get("sql_table_name", v_name).strip(";")
                    path_parts = [p.replace('"', '').replace('`', '').strip() for p in table_name.split(".")]
                    physical_table = path_parts[-1] if path_parts else v_name
                    if len(path_parts) == 1:
                        path = [env_db, env_schema, path_parts[0]]
                    elif len(path_parts) == 2:
                        path = [env_db, path_parts[0], path_parts[1]]
                    else:
                        path = path_parts
                    source = {"connectionId": env_conn, "kind": "warehouse-table", "path": path}
                columns = []
                metrics = []
                all_dims = view_def.get("dimensions", []) + view_def.get("dimension_groups", [])
                for dim in all_dims:
                    f = convert_formula(dim.get("sql"), physical_table, dim["name"], dim.get("type"), known_metrics)
                    if f:
                        columns.append({"id": dim["name"], "name": dim.get("label", dim["name"]), "formula": f})
                for meas in view_def.get("measures", []):
                    m_type = meas.get("type", "number")
                    inner_f = convert_formula(meas.get("sql"), physical_table, meas["name"], m_type, known_metrics)
                    if not inner_f and m_type == 'count':
                        f = "Count()"
                    elif inner_f:
                        if m_type == 'sum': f = f"Sum({inner_f})"
                        elif m_type == 'average': f = f"Avg({inner_f})"
                        elif m_type == 'count': f = f"Count({inner_f})"
                        elif m_type == 'count_distinct': f = f"CountDistinct({inner_f})"
                        elif m_type == 'min': f = f"Min({inner_f})"
                        elif m_type == 'max': f = f"Max({inner_f})"
                        else: f = inner_f
                    else:
                        f = ""
                    if f:
                        metrics.append({"id": meas["name"], "name": meas.get("label", meas["name"]), "formula": f})
                order = [c["id"] for c in columns]
                elements[v_name] = {"id": v_name, "kind": "table", "source": source, "name": v_name, "columns": columns, "metrics": metrics, "relationships": [], "order": order}

        for explore_name, explore_def in explores.items():
            base_view = explore_def.get("from") or explore_name
            add_element(base_view)
            for join in explore_def.get("joins", []):
                join_view = join.get("from") or join.get("name")
                if not join_view: continue
                add_element(join_view)
                sql_on = join.get("sql_on", "")
                join_parts = extract_simple_join(sql_on)
                if join_parts:
                    t1, f1, t2, f2 = join_parts
                    add_element(t1)
                    add_element(t2)
                    rel_type = (join.get("relationship") or "many_to_one").strip().lower()
                    if rel_type != "many_to_one":
                        print("Warning: Sigma API supports only many_to_one. Skipping relationship '%s' (LookML: %s)." % (join.get("name", t2), rel_type), file=sys.stderr)
                    else:
                        elements[t1]["relationships"].append({
                            "id": make_id(),
                            "targetElementId": t2,
                            "keys": [{"sourceColumnId": f1, "targetColumnId": f2}],
                            "name": join.get("name", t2)
                        })

        sigma_model = {
            "name": model_name_from_file,
            "folderId": env_folder,
            "schemaVersion": 1,
            "pages": [{"id": make_id(), "name": f"{model_name_from_file} Canvas", "elements": list(elements.values())}]
        }
        out_path = os.path.join(output_dir, f"{model_name_from_file}_unified_model.json")
        with open(out_path, "w") as f:
            json.dump(sigma_model, f, indent=2)
        written_basenames.add(os.path.basename(out_path))
        print(f"Generated compliant JSON Sigma Data Model: {out_path}")

    for f in os.listdir(output_dir):
        if f.endswith("_unified_model.json") and f not in written_basenames:
            try:
                os.remove(os.path.join(output_dir, f))
                print(f"Removed stale: {os.path.join(output_dir, f)}", file=sys.stderr)
            except OSError: pass

if __name__ == "__main__":
    main()
