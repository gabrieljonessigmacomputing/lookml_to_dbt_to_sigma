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

def main():
    lookml_dir = sys.argv[1] if len(sys.argv) > 1 else "lookml"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "sigma_model"
    
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
                table_name = view_def.get("sql_table_name", v_name).strip(";")
                path = [p.replace('"', '').strip() for p in table_name.split(".")]
                
                columns = []
                for dim in view_def.get("dimensions", []):
                    columns.append({"id": dim["name"], "formula": dim.get("sql", f"[{dim['name']}]")})
                for meas in view_def.get("measures", []):
                    columns.append({"id": meas["name"], "formula": meas.get("sql", f"[{meas['name']}]")})

                elements[v_name] = {
                    "id": v_name,
                    "kind": "table",
                    "source": {"kind": "warehouse-table", "path": path},
                    "name": v_name,
                    "columns": columns,
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
                
                # Ensure elements exist
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
        print(f"Generated unified Sigma Explore Model with relationships: {out_path}")

if __name__ == "__main__":
    main()
