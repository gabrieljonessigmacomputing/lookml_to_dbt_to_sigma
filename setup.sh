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
MANIFEST_DATABASE=MY_DB
MANIFEST_SCHEMA=PUBLIC
MANIFEST_TABLE_PREFIX=
MANIFEST_TABLE_SUFFIX=
API_URL=https://api.sigmacomputing.com/v2
SIGMA_DOMAIN=my-org.sigmacomputing.com
API_CLIENT_ID=dummy-client-id
API_SECRET=dummy-secret
CONNECTION_ID=dummy-connection-id
SIGMA_FOLDER_ID=dummy-folder-id
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
            if not name:
                continue

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

    print(f"Wrote semantic manifest with {len(semantic_models)} semantic model(s) to {manifest_path}")

if __name__ == "__main__":
    main()
EOF
chmod +x tools/generate_semantic_manifest.py

# ---------- tools/patch_semantic_models.py ----------
cat > tools/patch_semantic_models.py << 'EOF'
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
    model_name_arg = sys.argv[4] if len(sys.argv) > 4 else "consolidated_model"

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
    
    # NEW LOGIC: Consolidate all individual yaml files into one list
    all_semantic_models = []

    for filepath in yaml_files:
        with open(filepath, "r") as f:
            data = yaml.safe_load(f) or {}

        for sm in data.get("semantic_models", []):
            sm_name = sm.get("name")
            entities = sm.setdefault("entities", [])
            dimensions = sm.setdefault("dimensions", [])
            model_rels = relationships.get(sm_name, {})

            pk_name = model_rels.get("primary_key") or "id"
            if not any(e.get("type") == "primary" for e in entities):
                entities.append({"name": pk_name, "type": "primary", "expr": pk_name})
                sm["dimensions"] = [d for d in dimensions if d.get("name") != pk_name]

            for fk in model_rels.get("foreign_keys", []):
                fk_field, target = fk["field"], fk["to"]
                if not any(e.get("name") == target and e.get("type") == "foreign" for e in entities):
                    entities.append({"name": target, "type": "foreign", "expr": fk_field})
                    sm["dimensions"] = [d for d in sm["dimensions"] if d.get("name") != fk_field]

            all_semantic_models.append(sm)
            report["models_patched"].append(sm_name)

        # Cleanup the individual files so we don't have duplicates
        try:
            os.remove(filepath)
        except OSError:
            pass

    # Write the consolidated model out to a single file named after the LookML folder
    consolidated_path = os.path.join(semantic_dir, f"{model_name_arg}.yml")
    if all_semantic_models:
        with open(consolidated_path, "w") as f:
            yaml.dump({"semantic_models": all_semantic_models}, f, sort_keys=False)

    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w") as f: json.dump(report, f, indent=2)
    print(f"Patching complete. Consolidated into {consolidated_path}. See {report_file} for details.")

if __name__ == "__main__":
    main()
EOF
chmod +x tools/patch_semantic_models.py

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
name: LookML → Sigma (Multi-Model TEST mode)

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
            # Pass $model_name so the Python script names the consolidated yaml correctly
            python tools/patch_semantic_models.py "$CURRENT_LOOKML_DIR" "$SEMANTIC_MODELS_DIR" "$REPORT_FILE" "$model_name"
            
            echo "3. Generating Semantic Manifest..."
            python tools/generate_semantic_manifest.py "$SEMANTIC_MODELS_DIR" "$SEMANTIC_MANIFEST_FILE"
            
            echo "4. Translating to Sigma JSON Data Model..."
            (
              cd sigma_converter/sigma_converter
              export OUTPUT_DIR="./output_$model_name"
              export SIGMA_MODEL_DIR="../../sigma_model/$model_name"
              export DAG_FILE="./output_$model_name/dag.json"
              export SOURCE_DIR="${{ github.workspace }}/$SEMANTIC_MODELS_DIR"
              export SEMANTIC_MANIFEST_FILE="${{ github.workspace }}/out/${model_name}_manifest.json"
              
              mkdir -p "$OUTPUT_DIR" "$SIGMA_MODEL_DIR"
              
              node src/main.js
            )
          done

      - name: Commit and push generated Sigma models
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          
          git add sigma_model/ out/
          git commit -m "Auto-generated Sigma models from multi-directory LookML" || echo "No changes to commit"
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

echo "Setup complete! Once pushed, check GitHub Actions for the consolidated multi-model magic."