#!/usr/bin/env python3
import sys
import glob
import yaml
import json
import os
import re
import lkml

def extract_simple_join(sql_on):
    if not sql_on: return None
    match = re.match(r'^\s*\$\{([^.]+)\.([^}]+)\}\s*=\s*\$\{([^.]+)\.([^}]+)\}\s*$', sql_on)
    return match.groups() if match else None

def main():
    lookml_dir = sys.argv[1] if len(sys.argv) > 1 else "lookml"
    semantic_dir = sys.argv[2] if len(sys.argv) > 2 else "lookml/semantic_models"
    report_file = sys.argv[3] if len(sys.argv) > 3 else "out/migration_report.json"

    report = {"models_patched": [], "joins_mapped": [], "warnings": []}
    explores = {}
    
    for filepath in glob.glob(f"{lookml_dir}/**/*.lkml", recursive=True):
        with open(filepath, 'r') as f:
            try:
                parsed = lkml.load(f)
                for explore in parsed.get("explores", []):
                    explore_name = explore.get("name")
                    if not explore_name:
                        report["warnings"].append(f"Explore missing name in {filepath}")
                        continue
                    explores[explore_name] = explore
            except Exception as e:
                report["warnings"].append(f"Failed to parse {filepath}: {str(e)}")

    relationships = {}
    for explore_name, explore_def in explores.items():
        base_view = explore_def.get("from", explore_name)
        for join in explore_def.get("joins", []):
            join_view = join.get("from") or join.get("name")
            if not join_view:
                report["warnings"].append(f"Join missing both 'from' and 'name' in explore '{explore_name}'")
                continue
                
            sql_on = join.get("sql_on")
            
            rel_type = join.get("relationship", "many_to_one")
            if rel_type not in ["one_to_one", "many_to_one"]:
                report["warnings"].append(f"Fan-out risk in '{explore_name}': join '{join_view}' uses '{rel_type}'.")

            join_parts = extract_simple_join(sql_on)
            if join_parts:
                t1, f1, t2, f2 = join_parts
                if t1 == base_view and t2 == join_view:
                    base_fk, target_pk = f1, f2
                elif t2 == base_view and t1 == join_view:
                    base_fk, target_pk = f2, f1
                else:
                    continue

                relationships.setdefault(base_view, {"foreign_keys": [], "primary_key": None})
                relationships[base_view]["foreign_keys"].append({"field": base_fk, "to": join_view})
                
                relationships.setdefault(join_view, {"foreign_keys": [], "primary_key": None})
                relationships[join_view]["primary_key"] = target_pk
                
                report["joins_mapped"].append(f"Mapped {base_view}.{base_fk} -> {join_view}.{target_pk}")
            else:
                report["warnings"].append(f"Could not map complex join in '{explore_name}' -> '{join_view}': {sql_on}")

    yaml_files = glob.glob(f"{semantic_dir}/**/*.yml", recursive=True) + glob.glob(f"{semantic_dir}/**/*.yaml", recursive=True)
    for filepath in yaml_files:
        with open(filepath, "r") as f:
            data = yaml.safe_load(f) or {}

        modified = False
        for sm in data.get("semantic_models", []):
            model_name = sm.get("name")
            entities = sm.setdefault("entities", [])
            dimensions = sm.setdefault("dimensions", [])
            model_rels = relationships.get(model_name, {})

            pk_name = model_rels.get("primary_key") or "id"
            if not any(e.get("type") == "primary" for e in entities):
                entities.append({"name": pk_name, "type": "primary", "expr": pk_name})
                sm["dimensions"] = [d for d in dimensions if d.get("name") != pk_name]
                modified = True

            for fk in model_rels.get("foreign_keys", []):
                fk_field, target = fk["field"], fk["to"]
                if not any(e.get("name") == target and e.get("type") == "foreign" for e in entities):
                    entities.append({"name": target, "type": "foreign", "expr": fk_field})
                    sm["dimensions"] = [d for d in sm["dimensions"] if d.get("name") != fk_field]
                    modified = True

            if modified: report["models_patched"].append(model_name)

        if modified:
            with open(filepath, "w") as f: yaml.dump(data, f, sort_keys=False)

    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w") as f: json.dump(report, f, indent=2)
    print(f"Patching complete. See {report_file} for audit details.")

if __name__ == "__main__":
    main()
