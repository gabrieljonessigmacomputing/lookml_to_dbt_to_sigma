# Per-connection models and multiple Sigma data models

## How many Sigma models get created?

- **One JSON file per LookML model file** (e.g. `training_ecommerce.model.lkml` → `training_ecommerce_unified_model.json`; `plugs.model.lkml` → `plugs_unified_model.json`). All explores in that model file are combined into one Sigma data model.
- **One Sigma data model per JSON file** when you run `create_model.py`.

So:
- One LookML model file (with one or more explores) → one JSON → one Sigma data model.
- Multiple LookML project folders under `looker_files/` (e.g. `looker_files/plugs/`, `looker_files/looker_bq_qwiklab_ecommerce/`) → one or more JSONs per folder (one per .model.lkml) → all created when you run `create_model.py`.

To see what will be created, list the JSONs:

```bash
find sigma_model -name "*.json"
```

**The JSON files are only written when the conversion runs.** If you change LookML or the converter and don’t see updated files or modified dates, run the conversion again: `./regenerate_sigma_json.sh` (or run `./setup.sh`). `create_model.py` only reads existing JSON and POSTs to Sigma; it does not regenerate the files.

`create_model.py` prints how many files it found and creates one data model per file.

**Naming convention:** The setup sample project is **`plugs`** only: `looker_files/plugs/` with `plugs.model.lkml`. If you have old folders like `looker_files/plugs_model/` or `looker_files/plugs_hands_on_labs_data/`, remove or rename them so only one plugs project exists; otherwise you get multiple “plugs” Sigma models.

---

## Sigma custom SQL (derived tables and fallback)

- **LookML derived tables** (views with `derived_table: { sql: "..." }`) are converted to Sigma elements with source `kind: "sql"` and the SQL in `statement`. Column formulas use `[Custom SQL/column_id]` as in Sigma’s API.
- **Base tables**: We normally use `kind: "warehouse-table"` and `path`. If the Sigma API rejects the create (e.g. “no ui option” for BigQuery tables), `create_model.py` retries by converting those elements to `kind: "sql"` with `statement: "SELECT * FROM \`project.dataset.table\`"` and rewrites column/metric formulas to `[Custom SQL/...]`.
- **Sigma custom SQL limitations** (per Sigma docs): single SELECT; no DML/DDL; no ordinal refs (e.g. `$1`); custom SQL runs as a subquery, so statements that can’t run in a subquery/CTE may fail. If a derived table’s SQL is very complex, you may need to simplify it for Sigma.

When a create fails, `create_model.py` prints the full Sigma API status and response body so you can see the exact error and adjust the payload if needed.

---

## Where the script looks for LookML

The conversion runs **only** over **`looker_files/`** (each subdir = one project; one Sigma JSON per `.model.lkml` file, combining all explores in that file). Previously it also used lookml/; that is no longer used.

The conversion runs over **every project folder** under:

- **`lookml/`** (or whatever `LOOKML_DIR` is in `.env`)
- **`looker_files/`** — each subdir is one project, e.g. `looker_files/plugs/`, `looker_files/looker_bq_qwiklab_ecommerce/`

Each such folder is treated as one “model” (e.g. `plugs`). Inside it, the script finds all `.lkml` in **`views/`**, **`models/`**, and recursively; one Sigma JSON per `.model.lkml` file.

---

## Adding another LookML model with its own connection

Each LookML **project folder** under `looker_files/` (e.g. `looker_files/acme_retail`) can use a different Sigma **connection ID**. Steps:

### 1. Add the LookML

- **Option A:** Put project folders under `looker_files/`, each with `views/` and `models/`. Example: `looker_files/plugs/views/`, `looker_files/plugs/models/`.
- **Option B:** Import a zip: `./import_lookml.sh path/to/acme_retail.zip`. That creates a folder under `lookml/` from the zip; move it to `looker_files/<name>/` if you want it converted (conversion only reads `looker_files/`).

### 2. Set the connection for that model in `.env`

Use a **per-model** connection variable so this model doesn’t use the default:

- Name format: `CONNECTION_ID_<MODEL_NAME>`.
- `<MODEL_NAME>` = folder name in **UPPERCASE**, with hyphens and spaces as **underscores**.

Examples:

| LookML folder        | .env variable               |
|----------------------|-----------------------------|
| `looker_files/plugs` | `CONNECTION_ID_PLUGS` |
| `lookml/acme_retail` | `CONNECTION_ID_ACME_RETAIL` |
| `lookml/my-project`  | `CONNECTION_ID_MY_PROJECT`  |

In `.env`:

```bash
# Default (used if no per-model variable is set)
CONNECTION_ID_DEFAULT=your-default-connection-uuid

# Per-model overrides
CONNECTION_ID_PLUGS=uuid-for-plugs-warehouse
CONNECTION_ID_ACME_RETAIL=uuid-for-acme-warehouse
```

### 3. Run conversion and create models

```bash
./setup.sh
```

Or, if you only added LookML and didn’t change tools:

```bash
set -a && source .env && set +a
for dir in lookml/*/; do
  model_name=$(basename "${dir%/}")
  mkdir -p "sigma_model/$model_name"
  python3 tools/build_sigma_explore_json.py "$dir" "sigma_model/$model_name" "$model_name"
done
```

Then run `create_model.py` (or answer **Y** when setup asks) to create one Sigma data model per JSON under `sigma_model/`. Each JSON uses the connection ID for its model (from `CONNECTION_ID_<MODEL_NAME>` or `CONNECTION_ID_DEFAULT`).

---

## Environment variables reference

Single reference for all environment variables used by setup, conversion, and `create_model.py`. See also `.env.example` in the repo root.

**Conversion / setup** (used by `tools/build_sigma_explore_json.py` and embedded scripts):

| Variable | Description |
|----------|-------------|
| `LOOKML_DIR` | Root directory for LookML (default `lookml`); conversion uses `looker_files/` only. |
| `MANIFEST_DATABASE` | Warehouse database name for table paths in generated JSON. |
| `MANIFEST_SCHEMA` | Warehouse schema name for table paths. |
| `MANIFEST_TABLE_PREFIX` | Optional prefix for table names in manifest. |
| `MANIFEST_TABLE_SUFFIX` | Optional suffix for table names in manifest. |
| `CONNECTION_ID_DEFAULT` | Default Sigma warehouse connection ID for all models. |
| `CONNECTION_ID_<FOLDER>` | Per-project connection (e.g. `CONNECTION_ID_PLUGS` for `looker_files/plugs/`). Folder name in uppercase, hyphens→underscores. |
| `SIGMA_FOLDER_ID` | Sigma folder ID for created data models. |
| `API_URL` | Sigma API base URL (e.g. `https://api.sigmacomputing.com/v2`). |
| `SIGMA_DOMAIN` | Sigma org domain (e.g. `my-org.sigmacomputing.com`). |
| `API_CLIENT_ID`, `API_SECRET` | Sigma API credentials (used by `create_model.py` if `SIGMA_*` not set). |
| `MODE`, `USER_FRIENDLY_COLUMN_NAMES`, `TEST_FLAG`, `FROM_CI_CD` | Optional flags written by setup; used by optional workflows. |

**create_model.py** (push JSON to Sigma):

| Variable | Description |
|----------|-------------|
| `SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET` | Sigma API credentials (preferred). |
| `API_CLIENT_ID`, `API_SECRET` | Fallback credentials if `SIGMA_*` not set (e.g. from generated `.env`). |
| `SIGMA_API_BASE_URL` | Base URL for Sigma API (default `https://api.sigmacomputing.com`). |
| `CREATE_MODEL_VERBOSE` | When set, print full API response for each created model. |

---

## Troubleshooting

- **Wrong connection for a project** — Ensure `CONNECTION_ID_<FOLDER>` in `.env` uses the folder name in uppercase with hyphens replaced by underscores (e.g. `CONNECTION_ID_PLUGS` for `looker_files/plugs/`). See the table in "Adding another LookML model with its own connection."
- **Duplicate Sigma models for the same project** — Remove extra LookML project folders so only one folder per logical project exists under `looker_files/` (e.g. keep only `plugs/`, not `plugs_model/` or `plugs_hands_on_labs_data/`).
- **No models created / no JSON generated** — Run `./regenerate_sigma_json.sh` (or `setup.sh`). Ensure `looker_files/` exists and contains at least one project directory with `.model.lkml` files. If conversion reports "No .model.lkml files found", add a model file under `models/` that defines explores.
- **Local vs CI:** Local conversion uses only the Python converter (`tools/build_sigma_explore_json.py`). The optional GitHub Actions workflow may use a different path (dbt + Node); behavior and outputs can differ if both are used.

---

## Summary

| Goal                         | Action |
|-----------------------------|--------|
| One connection for everything | Set only `CONNECTION_ID_DEFAULT` in `.env`. |
| Different connection per LookML model | Add `CONNECTION_ID_<MODEL_NAME>` in `.env` (model name = folder name, uppercase, underscores). |
| More Sigma data models from same LookML | Add more **explores** in your `.model.lkml`; each explore becomes one JSON and one Sigma data model. |
| More Sigma data models from another project | Add another folder under `lookml/<name>/` (or import a zip) and set `CONNECTION_ID_<NAME>` if needed; re-run setup and `create_model.py`. |
