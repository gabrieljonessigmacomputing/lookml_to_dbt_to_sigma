#!/usr/bin/env python3
import sys, glob, yaml, json, os, re, lkml

def extract_simple_join(sql_on):
    if not sql_on: return None
    match = re.match(r'^\s*\$\{([^.]+)\.([^}]+)\}\s*=\s*\$\{([^.]+)\.([^}]+)\}\s*$', sql_on)
    return match.groups() if match else None

def main():
    lookml_dir = sys.argv[1] if len(sys.argv) > 1 else "lookml"
    semantic_dir = sys.argv[2] if len(sys.argv) > 2 else "lookml/semantic_models"
    report_file = sys.argv[3] if len(sys.argv) > 3 else "out/migration_report.json"
    model_name_arg = sys.argv[4] if len(sys.argv) > 4 else "consolidated_model"

    report = {"models_patched": [], "joins_mapped": [], "warnings": []}
    explores = {}
    
    for filepath in glob.glob(f"{lookml_dir}/**/*.lkml", recursive=True):
        with open(filepath, 'r') as f:
            try:
                parsed = lkml.load(f)
                for explore in parsed.get("explores", []):
                    e_name = explore.get("name")
                    if e_name: explores[e_name] = explore
            except: pass

    relationships = {}
    for explore_name, explore_def in explores.items():
        base_view = explore_def.get("from", explore_name)
        for join in explore_def.get("joins", []):
            join_view = join.get("from") or join.get("name")
            if not join_view: continue
            
            join_parts = extract_simple_join(join.get("sql_on"))
            if join_parts:
                t1, f1, t2, f2 = join_parts
                if t1 == base_view and t2 == join_view: base_fk, target_pk = f1, f2
                elif t2 == base_view and t1 == join_view: base_fk, target_pk = f2, f1
                else: continue

                relationships.setdefault(base_view, {"foreign_keys": [], "primary_key": None})
                relationships[base_view]["foreign_keys"].append({"field": base_fk, "to": join_view})
                relationships.setdefault(join_view, {"foreign_keys": [], "primary_key": None})
                relationships[join_view]["primary_key"] = target_pk

    yaml_files = glob.glob(f"{semantic_dir}/**/*.yml", recursive=True)
    all_sm = []

    for filepath in yaml_files:
        if os.path.basename(filepath) == f"{model_name_arg}.yml": continue
        try:
            with open(filepath, "r") as f: data = yaml.safe_load(f) or {}
            for sm in data.get("semantic_models", []):
                sm_name = sm.get("name")
                if not sm_name: continue
                
                entities, dimensions = sm.get("entities") or [], sm.get("dimensions") or []
                rels = relationships.get(sm_name, {})

                pk = rels.get("primary_key") or "id"
                if not any(e.get("type") == "primary" for e in entities):
                    entities.append({"name": pk, "type": "primary", "expr": pk})
                    dimensions = [d for d in dimensions if d.get("name") != pk]

                for fk in rels.get("foreign_keys", []):
                    if not any(e.get("name") == fk["to"] and e.get("type") == "foreign" for e in entities):
                        entities.append({"name": fk["to"], "type": "foreign", "expr": fk["field"]})
                        dimensions = [d for d in dimensions if d.get("name") != fk["field"]]

                sm["entities"], sm["dimensions"] = entities, dimensions
                all_sm.append(sm)
            os.remove(filepath)
        except: pass

    if all_sm:
        with open(os.path.join(semantic_dir, f"{model_name_arg}.yml"), "w") as f:
            yaml.dump({"semantic_models": all_sm}, f, sort_keys=False)

if __name__ == "__main__": main()
