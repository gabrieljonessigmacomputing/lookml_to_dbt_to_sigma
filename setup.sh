#!/usr/bin/env bash
set -euo pipefail
# Always run from repo root so lookml/, sigma_model/, .env are in the right place
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Directories: only create if missing (never remove or replace). Files: create or overwrite.
mkdir -p lookml tools .github/workflows out sigma_model

# ---------- Execute User's LookML Script ----------
echo "Running create_lookml.sh..."
if [ -f "./create_lookml.sh" ]; then
  bash ./create_lookml.sh
else
  echo "WARNING: create_lookml.sh not found in the current directory. Skipping LookML generation."
fi

# ---------- .env ----------
cat > .env << 'EOF'
LOOKML_DIR=lookml
# Project subdirs under lookml/ and looker_files/ (if present) are each converted; use looker_files for Looker export layout
MANIFEST_DATABASE=RETAIL
MANIFEST_SCHEMA=PLUGS_ELECTRONICS
MANIFEST_TABLE_PREFIX=
MANIFEST_TABLE_SUFFIX=
API_URL=https://api.sigmacomputing.com/v2
SIGMA_DOMAIN=my-org.sigmacomputing.com
API_CLIENT_ID=dummy-client-id
API_SECRET=dummy-secret
CONNECTION_ID_DEFAULT=bee6615c-7d11-435c-8819-e32207b27fe4
CONNECTION_ID_LOOKER_BQ_QWIKLAB_ECOMMERCE=b4a339ca-32ef-4ea9-a8b8-1525a27322cb
# Optional: per-model connection (CONNECTION_ID_<FOLDER_NAME>, folder name uppercase. E.g. CONNECTION_ID_PLUGS for looker_files/plugs/)
SIGMA_FOLDER_ID=23xVmjTE4gZP7P6Wnpi4rA
MODE=initial
USER_FRIENDLY_COLUMN_NAMES=true
TEST_FLAG=true
FROM_CI_CD=false
EOF

# ---------- tools/generate_semantic_manifest.py ----------
cat > tools/generate_semantic_manifest.py << 'EOF'
#!/usr/bin/env python3
import os
import sys
import json
import glob
import yaml

def main():
    semantic_dir = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SEMANTIC_MODELS_DIR", "lookml/semantic_models")
    manifest_path = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("SEMANTIC_MANIFEST_FILE", "out/semantic_manifest.json")

    database = os.environ["MANIFEST_DATABASE"]
    schema = os.environ["MANIFEST_SCHEMA"]
    prefix = os.environ.get("MANIFEST_TABLE_PREFIX", "")
    suffix = os.environ.get("MANIFEST_TABLE_SUFFIX", "")

    semantic_models = []

    yaml_paths = glob.glob(os.path.join(semantic_dir, "**/*.yml"), recursive=True) + \
                 glob.glob(os.path.join(semantic_dir, "**/*.yaml"), recursive=True)

    for path in yaml_paths:
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

        for sm in data.get("semantic_models", []):
            name = sm.get("name")
            if not name: continue

            table = (prefix + name + suffix).upper()
            semantic_models.append({
                "name": name,
                "node_relation": {
                    "database": database,
                    "schema_name": schema,
                    "relation_name": f"{database}.{schema}.{table}",
                }
            })

    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, "w") as f:
        json.dump({"semantic_models": semantic_models}, f, indent=2)

if __name__ == "__main__":
    main()
EOF
chmod +x tools/generate_semantic_manifest.py

# ---------- tools/patch_semantic_models.py ----------
cat > tools/patch_semantic_models.py << 'EOF'
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
EOF
chmod +x tools/patch_semantic_models.py

# ---------- tools/build_sigma_explore_json.py ----------
cat > tools/build_sigma_explore_json.py << 'EOF'
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
EOF
chmod +x tools/build_sigma_explore_json.py

# ---------- requirements.txt ----------
# Python tools + create_model.py (Sigma API)
cat > requirements.txt << 'EOF'
pyyaml
lkml
requests
EOF

echo "Installing Python dependencies..."
pip install -r requirements.txt

# ---------- Run conversion locally (generates sigma_model/*.json) ----------
echo "Running LookML → Sigma conversion..."
set -a
# shellcheck source=/dev/null
source .env
set +a
mkdir -p out sigma_model
# Process looker_files/ only. Dirs: only create (mkdir -p); never remove or replace.
for dir in looker_files/*/; do
  [ ! -d "$dir" ] && continue
  dir="${dir%/}"
  model_name="$(basename "$dir")"
  echo "--- Processing: $model_name ---"
  mkdir -p "sigma_model/$model_name"
  python3 tools/build_sigma_explore_json.py "$dir" "sigma_model/$model_name" "$model_name"
done
echo "Sigma JSON output is in: $(pwd)/sigma_model/"
ls -la sigma_model/
echo "To add more LookML or use a different connection per model: see CONNECTIONS.md."

# ---------- .github/workflows/lookml_to_sigma.yml ----------
cat > .github/workflows/lookml_to_sigma.yml << 'EOF'
name: LookML → Sigma (JSON Only Mode)

on:
  push:
    branches: [ main ]
  workflow_dispatch: {}

jobs:
  lookml_to_sigma:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Ensure output directory exists
        run: mkdir -p out sigma_model

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'
          cache: 'pip'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Set up Node for Sigma converter
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Clone Sigma dbt_semantics_to_sigma
        run: git clone --depth 1 https://github.com/sigmacomputing/dbt_semantics_to_sigma.git sigma_converter

      - name: Install Sigma converter dependencies
        working-directory: sigma_converter/sigma_converter
        run: npm install

      - name: Process each LookML subdirectory
        run: |
          set -a; source .env; set +a
          mkdir -p out sigma_model
          for dir in looker_files/*/; do
            [ ! -d "$dir" ] && continue
            dir=${dir%/}
            model_name=$(basename "$dir")
            echo "=========================================="
            echo "Processing Sigma Model: $model_name"
            echo "=========================================="
            export CURRENT_LOOKML_DIR="$dir"
            export SEMANTIC_MODELS_DIR="$dir/semantic_models"
            export SEMANTIC_MANIFEST_FILE="out/${model_name}_manifest.json"
            export REPORT_FILE="out/${model_name}_report.json"
            if command -v dbtc &>/dev/null; then
              echo "1. Converting LookML -> dbt Semantic Layer"
              ( cd "$dir" && rm -rf semantic_models && dbtc convert-lookml || true )
              if [ -d "$SEMANTIC_MODELS_DIR" ]; then
                find "$SEMANTIC_MODELS_DIR" -name "*.yaml" -exec bash -c 'mv "$1" "${1%.yaml}.yml"' _ {} \;
                echo "2. Patching and Consolidating Relationships..."
                python tools/patch_semantic_models.py "$CURRENT_LOOKML_DIR" "$SEMANTIC_MODELS_DIR" "$REPORT_FILE" "$model_name"
                echo "3. Generating Semantic Manifest..."
                python tools/generate_semantic_manifest.py "$SEMANTIC_MODELS_DIR" "$SEMANTIC_MANIFEST_FILE"
                echo "4. Translating via Node Converter..."
                ( cd sigma_converter/sigma_converter && export OUTPUT_DIR="./output_$model_name" SIGMA_MODEL_DIR="../../out/temp_yaml_dump/$model_name" DAG_FILE="./output_$model_name/dag.json" SOURCE_DIR="${{ github.workspace }}/$SEMANTIC_MODELS_DIR" SEMANTIC_MANIFEST_FILE="${{ github.workspace }}/out/${model_name}_manifest.json" && mkdir -p "$OUTPUT_DIR" "$SIGMA_MODEL_DIR" && node src/main.js )
              fi
            else
              echo "dbtc not installed; skipping dbt semantic layer and Node converter (Sigma JSON will still be generated from LookML)."
            fi
            echo "5. Generating Sigma JSON from LookML..."
            python tools/build_sigma_explore_json.py "$CURRENT_LOOKML_DIR" "sigma_model/$model_name" "$model_name"
          done

      - name: Commit and push strictly generated JSON
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          
          git add sigma_model/
          git commit -m "Auto-generated Unified JSON Sigma Data Models" || echo "No changes to commit"
          git push
EOF

echo "Setup complete. Sigma JSON files are in sigma_model/."
echo ""
read -r -p "Run create_model.py to push a data model to Sigma API? (y/N) " answer
case "${answer:-n}" in
  [yY]|[yY][eE][sS])
    if [ -f "./create_model.py" ]; then
      python3 create_model.py
    else
      echo "create_model.py not found."
    fi
    ;;
  *) echo "Skipped create_model.py." ;;
esac
echo ""
read -r -p "Push to GitHub now? (y/N) " answer
case "${answer:-n}" in
  [yY]|[yY][eE][sS])
    if [ -f "./push_to_git.sh" ]; then
      bash ./push_to_git.sh
    else
      echo "push_to_git.sh not found."
    fi
    ;;
  *) echo "Skipped push to GitHub." ;;
esac
