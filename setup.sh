#!/usr/bin/env bash
set -euo pipefail

mkdir -p lookml tools .github/workflows out

# ---------- README.md ----------
cat > README.md << 'EOF'
# LookML → dbt Semantic Layer → Sigma (TEST mode)

This repo is a starter project to translate LookML modeling into Sigma Data Models
via the dbt Semantic Layer, using only:

- LookML files in `lookml/`
- A simple `.env` configuration
- A GitHub Actions workflow

No dbt profile or warehouse credentials are required. The pipeline runs in **TEST mode**,
so it does **not** make real Sigma API calls by default.
EOF

# ---------- .env ----------
cat > .env << 'EOF'
LOOKML_DIR=lookml
SEMANTIC_MODELS_DIR=lookml/semantic_models
SEMANTIC_MANIFEST_FILE=out/semantic_manifest.json
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
import json
import glob
import yaml

def main():
    semantic_dir = os.environ.get("SEMANTIC_MODELS_DIR", "lookml/semantic_models")
    manifest_path = os.environ.get("SEMANTIC_MANIFEST_FILE", "out/semantic_manifest.json")

    database = os.environ["MANIFEST_DATABASE"]
    schema = os.environ["MANIFEST_SCHEMA"]
    prefix = os.environ.get("MANIFEST_TABLE_PREFIX", "")
    suffix = os.environ.get("MANIFEST_TABLE_SUFFIX", "")

    semantic_models = []

    yaml_paths = glob.glob(os.path.join(semantic_dir, "*.yml")) + \
                 glob.glob(os.path.join(semantic_dir, "*.yaml"))

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

# ---------- tools/patch_semantic_models.py (THE NEW FIX) ----------
cat > tools/patch_semantic_models.py << 'EOF'
#!/usr/bin/env python3
import glob
import yaml

for filepath in glob.glob("lookml/semantic_models/*.yml"):
    with open(filepath, "r") as f:
        data = yaml.safe_load(f) or {}

    modified = False
    for sm in data.get("semantic_models", []):
        entities = sm.setdefault("entities", [])
        has_primary = any(e.get("type") == "primary" for e in entities)

        if not has_primary:
            # Add 'id' as a primary entity so Sigma can join tables
            entities.append({"name": "id", "type": "primary"})
            modified = True

            # Remove 'id' from dimensions to avoid dbt duplicate name errors
            dims = sm.get("dimensions", [])
            sm["dimensions"] = [d for d in dims if d.get("name") != "id"]

    if modified:
        with open(filepath, "w") as f:
            yaml.dump(data, f, sort_keys=False)
        print(f"Patched {filepath}: Upgraded 'id' to primary entity.")
EOF
chmod +x tools/patch_semantic_models.py

# ---------- requirements.txt ----------
cat > requirements.txt << 'EOF'
numpy<2
dbt-core==1.5.*
pyyaml
git+https://github.com/dbt-labs/dbt-converter.git@master
EOF

# ---------- .github/workflows/lookml_to_sigma.yml ----------
cat > .github/workflows/lookml_to_sigma.yml << 'EOF'
name: LookML → Sigma (TEST mode, no DB, no Sigma API)

on:
  push:
    branches: [ main ]
  workflow_dispatch: {}

jobs:
  lookml_to_sigma:
    runs-on: ubuntu-latest

    # This line allows the GitHub Action bot to push code back to your repo
    permissions:
      contents: write

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Ensure output directory exists
        run: mkdir -p out

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.9'
          cache: 'pip'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run LookML → dbt semantic conversion
        run: |
          set -a; source .env; set +a
          cd "$LOOKML_DIR"
          rm -rf semantic_models
          dbtc convert-lookml
          
          # Rename .yaml to .yml
          echo "Renaming .yaml to .yml..."
          cd semantic_models
          for f in *.yaml; do
            [ -e "$f" ] && mv "$f" "${f%.yaml}.yml"
          done
          
          cd ../..
          python tools/patch_semantic_models.py

      - name: Generate stub semantic_manifest.json
        run: |
          set -a; source .env; set +a
          python tools/generate_semantic_manifest.py

      - name: Set up Node for Sigma converter
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Clone Sigma dbt_semantics_to_sigma
        run: git clone --depth 1 https://github.com/sigmacomputing/dbt_semantics_to_sigma.git sigma_converter

      - name: Install Sigma converter dependencies
        working-directory: sigma_converter/sigma_converter
        run: npm install

      - name: Run dbt_semantics_to_sigma in TEST mode
        working-directory: sigma_converter/sigma_converter
        run: |
          set -a; source ../../.env; set +a
          cp ../../.env .env
          
          export OUTPUT_DIR="./output"
          export SEMANTIC_MANIFEST_FILE="${{ github.workspace }}/out/semantic_manifest.json"
          export SOURCE_DIR="${{ github.workspace }}/lookml/semantic_models"
          export SIGMA_MODEL_DIR="./sigma_model"
          export DAG_FILE="./output/dag.json"
          export TEST_FLAG=true
          export FROM_CI_CD=false
          
          node src/main.js

      - name: List generated Sigma model specs
        working-directory: sigma_converter/sigma_converter
        run: ls -R sigma_model || echo "No sigma_model directory created."

      # --- THE NEW FIX: PUSH FILES BACK TO REPO ---
      - name: Commit and push generated Sigma models
        run: |
          # Pull the generated models out of the converter tool and into the main repo folder
          cp -r sigma_converter/sigma_converter/sigma_model ./sigma_model
          
          # Configure Git as the GitHub Actions bot
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          
          # Add, commit, and push!
          git add sigma_model/
          git commit -m "Auto-generated Sigma models" || echo "No changes to commit"
          git push
EOF

# ---------- lookml/ecommerce.model.lkml ----------
cat > lookml/ecommerce.model.lkml << 'EOF'
connection: "your_connection_name"

include: "*.view.lkml"

explore: orders {
  from: orders
  label: "Orders"
  description: "Orders explore generated from Sigma pipeline test"
}

explore: customers {
  from: customers
  label: "Customers"
  description: "Customers explore generated from Sigma pipeline test"
}
EOF

# ---------- lookml/orders.view.lkml ----------
cat > lookml/orders.view.lkml << 'EOF'
view: orders {
  sql_table_name: my_db.public.orders ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: order_date {
    type: date
    sql: ${TABLE}.order_date ;;
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  measure: total_revenue {
    type: sum
    sql: ${TABLE}.revenue ;;
    value_format_name: usd
  }
}
EOF

# ---------- lookml/customers.view.lkml ----------
cat > lookml/customers.view.lkml << 'EOF'
view: customers {
  sql_table_name: my_db.public.customers ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  measure: customer_count {
    type: count
  }
}
EOF

# ---------- Git Initialization & Force Push ----------
echo "Initializing Git repository and force-pushing to GitHub..."

git init
git add .
git commit -m "Patch primary entities for Sigma converter" || true 
git branch -M main

git remote remove origin 2>/dev/null || true
git remote add origin https://github.com/gabrieljonessigmacomputing/lookml_to_dbt_to_sigma.git

git push -u origin main --force

echo "Done! Code is live and the GitHub Action should trigger."