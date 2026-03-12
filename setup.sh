#!/usr/bin/env bash
set -euo pipefail

mkdir -p lookml tools .github/workflows out sigma_model

# ---------- Execute User's LookML Script ----------
echo "Running create_lookml.sh..."
if [ -f "./create_lookml.sh" ]; then
  bash ./create_lookml.sh
else
  echo "WARNING: create_lookml.sh not found in the current directory. Skipping LookML generation."
fi

# ---------- Execute User's README Script ----------
echo "Running make_readme.sh..."
if [ -f "./make_readme.sh" ]; then
  bash ./make_readme.sh
else
  echo "WARNING: make_readme.sh not found in the current directory. Skipping README generation."
fi

# ---------- .env ----------
cat > .env << 'EOF'
LOOKML_DIR=lookml
MANIFEST_DATABASE=RETAIL
MANIFEST_SCHEMA=PLUGS_ELECTRONICS
MANIFEST_TABLE_PREFIX=
MANIFEST_TABLE_SUFFIX=
API_URL=https://api.sigmacomputing.com/v2
SIGMA_DOMAIN=my-org.sigmacomputing.com
API_CLIENT_ID=dummy-client-id
API_SECRET=dummy-secret
CONNECTION_ID=bee6615c-7d11-435c-8819-e32207b27fe4
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
    
    # Extract defaults from env
    env_db = os.environ.get("MANIFEST_DATABASE", "RETAIL")
    env_schema = os.environ.get("MANIFEST_SCHEMA", "PLUGS_ELECTRONICS")
    env_conn = os.environ.get("CONNECTION_ID", "bee6615c-7d11-435c-8819-e32207b27fe4")
    env_folder = os.environ.get("SIGMA_FOLDER_ID", "23xVmjTE4gZP7P6Wnpi4rA")

    views = {}
    explores = {}
    known_metrics = set()

    # 1. Parse all LookML Views and Explores
    for filepath in glob.glob(f"{lookml_dir}/**/*.lkml", recursive=True):
        with open(filepath, 'r') as f:
            try:
                parsed = lkml.load(f)
                for view in parsed.get("views", []):
                    v_name = view.get("name")
                    if v_name: 
                        views[v_name] = view
                        # Collect all measure names so we can correctly tag them as [Metrics/...] later
                        for meas in view.get("measures", []):
                            known_metrics.add(meas["name"])
                            
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
                physical_table = path_parts[-1]
                
                if len(path_parts) == 1:
                    path = [env_db, env_schema, path_parts[0]]
                elif len(path_parts) == 2:
                    path = [env_db, path_parts[0], path_parts[1]]
                else:
                    path = path_parts
                
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
                    
                    # Apply proper Sigma Aggregation wrappers
                    if not inner_f and m_type == 'count':
                        f = "Count()"
                    elif inner_f:
                        if m_type == 'sum': f = f"Sum({inner_f})"
                        elif m_type == 'average': f = f"Avg({inner_f})"
                        elif m_type == 'count': f = f"Count({inner_f})"
                        elif m_type == 'count_distinct': f = f"CountDistinct({inner_f})"
                        elif m_type == 'min': f = f"Min({inner_f})"
                        elif m_type == 'max': f = f"Max({inner_f})"
                        else: f = inner_f  # 'number' types (e.g. Margins) remain untouched
                    else:
                        f = ""

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
EOF
chmod +x tools/build_sigma_explore_json.py

# ---------- requirements.txt ----------
cat > requirements.txt << 'EOF'
numpy<2
dbt-core==1.5.*
pyyaml
lkml
git+https://github.com/dbt-labs/dbt-converter.git@master
EOF

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
          
          for dir in "$LOOKML_DIR"/*/; do
            dir=${dir%/}
            if [ ! -d "$dir" ]; then continue; fi
            
            model_name=$(basename "$dir")
            echo "=========================================="
            echo "Processing Sigma Model: $model_name"
            echo "=========================================="
            
            export CURRENT_LOOKML_DIR="$dir"
            export SEMANTIC_MODELS_DIR="$dir/semantic_models"
            export SEMANTIC_MANIFEST_FILE="out/${model_name}_manifest.json"
            export REPORT_FILE="out/${model_name}_report.json"
            
            echo "1. Converting LookML -> dbt Semantic Layer"
            (
              cd "$dir"
              rm -rf semantic_models
              dbtc convert-lookml || true
              if [ -d "semantic_models" ]; then
                cd semantic_models
                find . -name "*.yaml" -exec bash -c 'mv "$1" "${1%.yaml}.yml"' _ {} \;
              fi
            )
            
            if [ ! -d "$SEMANTIC_MODELS_DIR" ]; then
              echo "No semantic models generated for $model_name. Skipping."
              continue
            fi
            
            echo "2. Patching and Consolidating Relationships..."
            python tools/patch_semantic_models.py "$CURRENT_LOOKML_DIR" "$SEMANTIC_MODELS_DIR" "$REPORT_FILE" "$model_name"
            
            echo "3. Generating Semantic Manifest..."
            python tools/generate_semantic_manifest.py "$SEMANTIC_MODELS_DIR" "$SEMANTIC_MANIFEST_FILE"
            
            echo "4. Translating via Node Converter (Isolating output)..."
            (
              cd sigma_converter/sigma_converter
              export OUTPUT_DIR="./output_$model_name"
              export SIGMA_MODEL_DIR="../../out/temp_yaml_dump/$model_name"
              export DAG_FILE="./output_$model_name/dag.json"
              export SOURCE_DIR="${{ github.workspace }}/$SEMANTIC_MODELS_DIR"
              export SEMANTIC_MANIFEST_FILE="${{ github.workspace }}/out/${model_name}_manifest.json"
              mkdir -p "$OUTPUT_DIR" "$SIGMA_MODEL_DIR"
              node src/main.js
            )

            echo "5. Generating UNIFIED Sigma JSON with Explore Relationships..."
            python tools/build_sigma_explore_json.py "$CURRENT_LOOKML_DIR" "sigma_model/$model_name"
          done

      - name: Commit and push strictly generated JSON
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          
          git add sigma_model/
          git commit -m "Auto-generated Unified JSON Sigma Data Models" || echo "No changes to commit"
          git push
EOF

# ---------- Execute Git Push Script ----------
echo "Running push_to_git.sh..."
if [ -f "./push_to_git.sh" ]; then
  bash ./push_to_git.sh
else
  echo "ERROR: push_to_git.sh not found. Skipping repository push phase."
  exit 1
fi

echo "Setup complete! Once pushed, check GitHub Actions for your pristine API-ready JSONs."