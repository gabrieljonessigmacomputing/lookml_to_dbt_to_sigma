# LookML → dbt Semantic Layer → Sigma (TEST mode)

This repo is a starter project to translate LookML modeling into Sigma Data Models
via the dbt Semantic Layer, using only:

- LookML files in `lookml/`
- A simple `.env` configuration
- A GitHub Actions workflow

No dbt profile or warehouse credentials are required. The pipeline runs in **TEST mode**,
so it does **not** make real Sigma API calls by default.

## How it works

1. **LookML → dbt semantic YAML**

   The GitHub Actions workflow runs:

   - `dbtc convert-lookml` from the `lookml/` directory
   - This creates `lookml/semantic_models/*.yml` (dbt Semantic Layer models + metrics)

2. **Semantic YAML → stub semantic_manifest.json**

   The script `tools/generate_semantic_manifest.py`:

   - Reads all `semantic_models/*.yml`
   - Uses `.env` to derive physical table names (database/schema/prefix/suffix)
   - Writes a minimal `out/semantic_manifest.json` matching dbt’s semantic manifest shape

3. **Semantic Layer → Sigma Data Model specs (TEST mode)**

   The workflow:

   - Clones `sigmacomputing/dbt_semantics_to_sigma`
   - Runs `node src/main.js` in TEST mode:
     - `SOURCE_DIR` = `lookml/semantic_models`
     - `SEMANTIC_MANIFEST_FILE` = `out/semantic_manifest.json`
     - `TEST_FLAG=true` so no real Sigma API calls are made
   - Writes Sigma Data Model specs to `sigma_converter/sigma_converter/sigma_model/*.yml`

## Getting started

1. Edit **`.env`** to set:
   - `MANIFEST_DATABASE`
   - `MANIFEST_SCHEMA`
   - optional `MANIFEST_TABLE_PREFIX` / `MANIFEST_TABLE_SUFFIX`

2. Put your LookML project in **`lookml/`**.

3. Commit and push to GitHub.

4. In GitHub, run the **“LookML → Sigma (TEST mode, no DB, no Sigma API)”** workflow.

5. Inspect generated Sigma model specs under:

   `sigma_converter/sigma_converter/sigma_model/`

When you’re confident in the specs, you can disable TEST mode by setting
`TEST_FLAG=false` in `.env` and letting the converter talk to Sigma’s APIs.
