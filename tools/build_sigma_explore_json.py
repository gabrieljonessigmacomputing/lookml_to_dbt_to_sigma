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

def convert_formula(lookml_sql, physical_table, field_name, field_type=None):
    """Converts LookML syntax to Sigma Data Model bracket syntax."""
    if not lookml_sql:
        if field_type == 'count' or field_type == 'count_distinct':
            return "Count()"
        if field_type == 'location':
            return "" # Location usually requires latitude/longitude pairs in Looker, omitting formula for safety
        return f"[{physical_table}/{field_name}]"
    
    # 1. Replace ${TABLE}."Column Name" with [TABLE_NAME/Column Name]
    s = re.sub(r'\$\{TABLE\}\."([^"]+)"', rf'[{physical_table}/\1]', lookml_sql)
    
    # 2. Replace ${TABLE}.column_name with [TABLE_NAME/column_name]
    s = re.sub(r'\$\{TABLE\}\.([a-zA-Z0-9_]+)', rf'[{physical_table}/\1]', s)
    
    # 3. Replace generic ${field_name} with [field_name]
    s = re.sub(r'\$\{([^}]+)\}', r'[\1]', s)
    
    return s

def main():
    lookml_dir = sys.argv[1] if len(sys.argv) > 1 else "lookml"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "sigma_model"
    
    # Extract defaults from env
    env_db = os.environ.get("MANIFEST_DATABASE", "RETAIL")
    env_schema = os.environ.get("MANIFEST_SCHEMA", "PLUGS_ELECTRONICS")
    env_conn = os.environ.get("CONNECTION_ID", "66YAkyjEZmOclq9LqxlKfm")
    env_folder = os.environ.get("SIGMA_FOLDER_ID", "23xVmjTE4gZP7P6Wnpi4rA")

    views = {}
    explores = {}

    # 1. Parse all LookML Views and Explores
    for filepath in glob.glob(f"{lookml_dir}/**/*.lkml", recursive=True):
        with open(filepath, 'r') as f:
            try:
                parsed = lkml.load(f)
                for view in parsed.get("views", []):
                    v_name = view.get("name")
                    if v_name: views[v_name] = view
                for explore in parsed.get("explores", []):
                    e_name = explore.get("name")
                    if e_name: explores[e_name] = explore
            except Exception as e:
                pass

    os.makedirs(output_dir, exist_ok=True)

    # 2. Build Sigma JSON for each Explore
    for explore_name, explore_def in explores.items():
        base_view = explore_def.get("from") or explore_name
        elements = {}

        def add_element(v_name):
            if v_name not in elements:
                view_def = views.get(v_name, {})
                
                # Extract the physical table name to use in Sigma formulas
                table_name = view_def.get("sql_table_name", v_name).strip(";")
                path_parts = [p.replace('"', '').replace('`', '').strip() for p in table_name.split(".")]
                physical_table = path_parts[-1] # The actual table name
                
                if len(path_parts) == 1:
                    path = [env_db, env_schema, path_parts[0]]
                elif len(path_parts) == 2:
                    path = [env_db, path_parts[0], path_parts[1]]
                else:
                    path = path_parts
                
                columns = []
                metrics = []
                
                # Grab both standard dimensions and dimension groups
                all_dims = view_def.get("dimensions", []) + view_def.get("dimension_groups", [])
                for dim in all_dims:
                    f = convert_formula(dim.get("sql"), physical_table, dim["name"], dim.get("type"))
                    if f: # Skip empty formulas like un-mapped locations
                        columns.append({"id": dim["name"], "name": dim.get("label", dim["name"]), "formula": f})
                
                for meas in view_def.get("measures", []):
                    f = convert_formula(meas.get("sql"), physical_table, meas["name"], meas.get("type"))
                    if f:
                        metrics.append({"id": meas["name"], "name": meas.get("label", meas["name"]), "formula": f})

                elements[v_name] = {
                    "id": v_name,
                    "kind": "table",
                    "source": {
                        "connectionId": env_conn,
                        "kind": "warehouse-table", 
                        "path": path
                    },
                    "name": v_name,
                    "columns": columns,
                    "metrics": metrics,
                    "relationships": []
                }

        # Add base view
        add_element(base_view)

        # Add relationships based on LookML joins
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
                
                elements[t1]["relationships"].append({
                    "id": make_id(),
                    "targetElementId": t2,
                    "keys": [{"sourceColumnId": f1, "targetColumnId": f2}],
                    "name": join.get("name", t2)
                })

        sigma_model = {
            "name": explore_name,
            "folderId": env_folder,
            "schemaVersion": 1,
            "pages": [{
                "id": make_id(),
                "name": f"{explore_name} Explore Canvas",
                "elements": list(elements.values())
            }]
        }

        out_path = os.path.join(output_dir, f"{explore_name}_unified_model.json")
        with open(out_path, "w") as f:
            json.dump(sigma_model, f, indent=2)
        print(f"Generated compliant JSON Sigma Data Model: {out_path}")

if __name__ == "__main__":
    main()
