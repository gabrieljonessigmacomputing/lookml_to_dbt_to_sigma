mkdir -p lookml tools .github/workflows out

# ---------- .env ----------
cat > .env << 'EOF'
# --- Paths (all relative to repo root) ---

# Root of your LookML project. The workflow will run dbtc convert-lookml from here.
LOOKML_DIR=lookml

# Where dbt-converter will write semantic models. This must match what dbtc does:
# it will create "semantic_models/" inside LOOKML_DIR.
SEMANTIC_MODELS_DIR=lookml/semantic_models

# Where we write the stub semantic_manifest.json
SEMANTIC_MANIFEST_FILE=out/semantic_manifest.json

# --- How to derive physical table names for each semantic model ---
# The semantic_manifest.json only needs database, schema, table names so that
# Prashant's converter can build Sigma "warehouse-table" paths.
#
# For each semantic model name N, we will use:
#   TABLE_NAME = (MANIFEST_TABLE_PREFIX + N + MANIFEST_TABLE_SUFFIX).upper()
#
# Example: if N = "orders", prefix="STG_", suffix="_V" => STG_ORDERS_V

MANIFEST_DATABASE=MY_DB
MANIFEST_SCHEMA=PUBLIC
MANIFEST_TABLE_PREFIX=
MANIFEST_TABLE_SUFFIX=

# --- Sigma converter env (can be dummy while TEST_FLAG=true) ---
# These are required by the converter, but will only be used in placeholder
# functions while TEST_FLAG=true.

API_URL=https://api.sigmacomputing.com/v2
SIGMA_DOMAIN=my-org.sigmacomputing.com
API_CLIENT_ID=dummy-client-id
API_SECRET=dummy-secret
CONNECTION_ID=dummy-connection-id
SIGMA_FOLDER_ID=dummy-folder-id

# --- Converter behavior ---
# Mode is "initial" for the first full run. You can later change to "update"
# if you want to mirror the dbt_semantics_to_sigma update mode behavior.
MODE=initial

# Must match the "user-friendly column names" setting on your Sigma connection
USER_FRIENDLY_COLUMN_NAMES=true

# SAFETY: keep test mode on; converter will not make real Sigma API calls.
TEST_FLAG=true

# SAFETY: do not commit/push from inside the converter.
FROM_CI_CD=false
EOF

# ---------- tools/generate_semantic_manifest.py ----------
cat > tools/generate_semantic_manifest.py << 'EOF'
#!/usr/bin/env python3
import os
import json
import glob

import yaml  # installed via pyyaml in the workflow

def main():
    semantic_dir = os.environ.get("SEMANTIC_MODELS_DIR", "lookml/semantic_models")
    manifest_path = os.environ.get("SEMANTIC_MANIFEST_FILE", "out/semantic_manifest.json")

    database = os.environ["MANIFEST_DATABASE"]
    schema = os.environ["MANIFEST_SCHEMA"]
    prefix = os.environ.get("MANIFEST_TABLE_PREFIX", "")
    suffix = os.environ.get("MANIFEST_TABLE_SUFFIX", "")

    semantic_models = []

    # Collect all semantic model YAMLs emitted by dbt-converter
    yaml_paths = glob.glob(os.path.join(semantic_dir, "*.yml")) + \
                 glob.glob(os.path.join(semantic_dir, "*.yaml"))

    for path in yaml_paths:
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}

        for sm in data.get("semantic_models", []):
            name = sm.get("name")
            if not name:
                continue

            # Derive physical table name from env + semantic model name
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

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Ensure output directory exists
        run: |
          mkdir -p out

      # ----------------------------------------------------
      # 1) LookML → dbt semantic models (YAML only)
      # ----------------------------------------------------
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt converter + YAML lib
        run: |
          pip install "dbt-core==1.5.*" dbt-metrics-converter pyyaml

      - name: Run LookML → dbt semantic conversion
        run: |
          set -a
          source .env
          set +a

          cd "$LOOKML_DIR"

          # Clean previous semantic_models, if any
          rm -rf semantic_models

          # This creates semantic_models/*.yml based only on LookML.
          dbtc convert-lookml

          echo "Generated semantic models under $LOOKML_DIR/semantic_models:"
          ls -R semantic_models

      # ----------------------------------------------------
      # 2) Stub semantic_manifest.json (no dbt/profile)
      # ----------------------------------------------------
      - name: Generate stub semantic_manifest.json
        run: |
          set -a
          source .env
          set +a

          python tools/generate_semantic_manifest.py

          echo "semantic_manifest.json contents:"
          cat "$SEMANTIC_MANIFEST_FILE"

      # ----------------------------------------------------
      # 3) dbt semantics → Sigma specs (TEST_FLAG=true)
      # ----------------------------------------------------
      - name: Set up Node for Sigma converter
        uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Clone Sigma dbt_semantics_to_sigma
        run: |
          git clone --depth 1 https://github.com/sigmacomputing/dbt_semantics_to_sigma.git sigma_converter

      - name: Install Sigma converter dependencies
        working-directory: sigma_converter/sigma_converter
        run: |
          npm ci

      - name: Run dbt_semantics_to_sigma in TEST mode (no Sigma API calls)
        working-directory: sigma_converter/sigma_converter
        run: |
          # Load env for path + naming config
          set -a
          source ../../.env
          set +a

          # Make the converter see the same .env at its expected location:
          # convert_semantics.js does `dotenv.config({ path: '../../../.env' })`,
          # so we copy our root .env here.
          cp ../../.env .env

          # Wire converter paths and behavior via environment variables,
          # matching what action.yml in that repo expects.
          export SEMANTIC_MANIFEST_FILE="$SEMANTIC_MANIFEST_FILE"
          export SOURCE_DIR="$SEMANTIC_MODELS_DIR"
          export SIGMA_MODEL_DIR="./sigma_model"
          export DAG_FILE="./output/dag.json"

          # Safety: stay in test mode, avoid CI/CD side effects
          export TEST_FLAG=true
          export FROM_CI_CD=false

          echo "Running dbt_semantics_to_sigma with:"
          echo "  SOURCE_DIR=$SOURCE_DIR"
          echo "  SEMANTIC_MANIFEST_FILE=$SEMANTIC_MANIFEST_FILE"
          echo "  TEST_FLAG=$TEST_FLAG"
          echo "  FROM_CI_CD=$FROM_CI_CD"

          node src/main.js

      - name: List generated Sigma model specs (sigma_model/)
        working-directory: sigma_converter/sigma_converter
        run: |
          echo "Generated Sigma model specs (from TEST mode):"
          if [ -d sigma_model ]; then
            ls -R sigma_model
          else
            echo "No sigma_model directory created."
          fi
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