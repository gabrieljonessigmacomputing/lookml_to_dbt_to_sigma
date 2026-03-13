# Codebase enhancement plan (full)

This document is the single reference for improvements: error handling, documentation, git simplification, edge cases, and dependency reduction. Sections 1–4 are from the original plan; sections 5–7 are additions.

---

## 1. Error handling and robustness

### 1.1 Env var mismatch (user-facing bug)

**Issue:** setup.sh and the generated .env set `API_CLIENT_ID` and `API_SECRET`, but create_model.py only reads `SIGMA_CLIENT_ID` and `SIGMA_CLIENT_SECRET`. After a fresh setup, running create_model.py fails with "Missing SIGMA_CLIENT_ID or SIGMA_CLIENT_SECRET" even though credentials are in .env.

**Recommendation:** Have create_model.py accept both: try `SIGMA_CLIENT_ID` / `SIGMA_CLIENT_SECRET` first, then fall back to `API_CLIENT_ID` / `API_SECRET`. No change to setup or .env; backward compatible.

### 1.2 setup.sh — Pre-flight checks

Add checks for `python3` and `pip` at the start; exit with a clear message if missing. Optionally check that `looker_files` exists and has at least one subdirectory before the conversion loop, and warn or exit 1 if there is nothing to convert.

### 1.3 tools/build_sigma_explore_json.py — Fail fast and report

- Validate that the given LookML directory exists and is a directory; exit with a clear error and non-zero exit if not.
- If after discovery there are no model files (or no valid ones), print to stderr and exit non-zero (e.g. 1).
- In the discovery loop, log parse failures to stderr instead of swallowing them; optionally exit 1 if any required model file failed to parse.

### 1.4 regenerate_sigma_json.sh — Validate input

Before the loop, check that `looker_files` exists and that `looker_files/*/` expands to at least one directory; otherwise print to stderr and exit 1.

### 1.5 create_lookml.sh — Harden

Add `set -e` (or `set -euo pipefail`) at the top so any failed command stops the script.

### 1.6 create_model.py — Timeout and optional retries

Use a reasonable `timeout=` on requests (e.g. 30–60 s). Optionally add a single retry for auth on connection error before exiting 1.

---

## 2. Documentation

### 2.1 Single env var reference

Add an **Environment variables** section (README, CONNECTIONS.md, or ENV.md) listing all vars: conversion/setup (LOOKML_DIR, MANIFEST_*, CONNECTION_ID_*, SIGMA_FOLDER_ID, API_URL, SIGMA_DOMAIN, etc.) and create_model.py (SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET, fallback API_* if 1.1 done, SIGMA_API_BASE_URL, CREATE_MODEL_VERBOSE). Optionally add .env.example with placeholder values and brief comments.

### 2.2 README — Quick start and troubleshooting

- Add a **Quick start** subsection: run setup.sh, edit .env, optionally run create_model.py.
- Add **Troubleshooting**: Missing SIGMA_CLIENT_ID → set it or use API_CLIENT_ID; No JSON files → run setup/regenerate and ensure looker_files/<project>/ exists; Multiple plugs models → see CONNECTIONS.md; Create model fails → check connection ID, custom SQL retry.
- In Prerequisites, add minimum versions (e.g. Python 3.9+, Bash 4+) if desired.

### 2.3 CONNECTIONS.md — Troubleshooting

Add a short Troubleshooting subsection (wrong connection, duplicate models, no models created) with pointers to env reference and README. Note that local conversion uses only the Python converter; CI may use a different path (dbt + Node).

### 2.4 Script headers (purpose / usage)

- create_lookml.sh: Add Purpose (create sample LookML under looker_files/plugs/; usually run by setup.sh) and Usage (./create_lookml.sh).
- push_to_git.sh: Add Purpose and Usage—or remove the script if git automation is dropped (see Section 5).

---

## 3. Comments and code clarity

### 3.1 setup.sh embedded Python

Add a short comment block above the first embedded Python listing the env vars that block expects (MANIFEST_*, etc.). For the conversion loop, add a one-line comment that it uses CONNECTION_ID_<model_key> and CONNECTION_ID_DEFAULT.

### 3.2 tools/build_sigma_explore_json.py

Add a module-level or "Environment variables" comment block listing each variable and its role.

### 3.3 create_model.py

Document SIGMA_API_BASE_URL and CREATE_MODEL_VERBOSE in the env reference and add one-line comments next to their os.getenv() calls.

---

## 4. Optional / lower priority

- requirements.txt: Pin versions for reproducible installs; document Python 3.9+ in README.
- CI workflow: Add a one-line note in README or CONNECTIONS that CI may use a different conversion path (dbt + Node) so results can differ from local.
- Tests: Optional smoke test (run build_sigma_explore_json.py on looker_files/plugs/ and assert at least one JSON is produced).

---

## 5. Git automation — simplify or remove

**Current state:**

- **push_to_git.sh** hardcodes `REPO_URL` to a specific GitHub repo; runs make_readme.sh, git init, git add -A, commit, sets origin, and push with force-with-lease and backup branches. User indicated this may not be necessary at all.
- **setup.sh** prompts "Push to GitHub now? (y/N)" at the end and runs push_to_git.sh if yes.
- **.github/workflows/lookml_to_sigma.yml** (embedded in setup.sh) runs on push to main; uses Node + dbt_semantics_to_sigma; has a step that commits and pushes only `sigma_model/` back to the repo.

**Recommendation — choose one of:**

- **Option A (remove git automation):**
  - Remove **push_to_git.sh** entirely.
  - Remove the "Push to GitHub now?" prompt and any invocation of push_to_git.sh from **setup.sh**.
  - **make_readme.sh** stays: useful standalone to regenerate README; document as "run when you want to refresh README."
  - **CI workflow:** Either stop generating it from setup (so no .github/workflows written by setup), or generate a **simplified** workflow that only runs conversion (Python-only: build_sigma_explore_json.py) and does **not** commit or push. Document: "If you use GitHub Actions, add your own workflow; this repo does not push for you."

- **Option B (keep git, don’t push by default):**
  - Keep push_to_git.sh but make it generic: no hardcoded REPO_URL; read from env (e.g. GIT_REMOTE_URL) or prompt/user doc: "Set your remote and run git push yourself."
  - Remove or soften the setup.sh prompt (e.g. "Push to GitHub? (y/N)" → default N, or remove the prompt and only document in README that users can commit/push manually).
  - Keep or simplify the generated workflow (no auto-commit/push step in workflow; workflow only runs conversion for CI visibility).

**Doc updates:** Wherever README/make_readme mentions "push to GitHub" or push_to_git.sh, update to match the chosen option (e.g. "To version your code, use git and push to your own remote" or remove references to push_to_git.sh).

---

## 6. Additional edge cases

- **Missing .env when not running setup:** regenerate_sigma_json.sh does `source .env`. If .env doesn’t exist (e.g. repo cloned, setup not run), `source .env` fails under `set -u`. **Enhancement:** In regenerate_sigma_json.sh, if .env is missing, print a clear message ("Run setup.sh first to create .env") and exit 1 instead of letting source fail.
- **Empty or invalid LookML files:** Empty .lkml files are skipped silently in build_sigma_explore_json.py; covered by 1.3 (log or fail). **Malformed JSON** in create_model.py is already handled (JSONDecodeError, continue and count failed).
- **Paths with spaces:** Shell loops use quoted vars (e.g. `"$dir"` in regenerate_sigma_json.sh). Ensure all script invocations pass paths quoted (e.g. `"$dir"`, `"sigma_model/$model_name"`). Python side uses argv and os.path; generally safe.
- **create_model.py run from another directory:** It uses `script_dir` to find sigma_model/ relative to the script location, so it works regardless of cwd. No change needed.
- **Unicode in view/explore names or SQL:** Rely on lkml/JSON; no explicit ASCII assumption. If any place assumes ASCII (e.g. regex), note in comments or add a test with non-ASCII names.
- **Very large projects:** Many explores or huge JSON could hit API or filesystem limits; document or add a note in Troubleshooting. Low priority.
- **CI workflow needs .env:** The workflow does `set -a; source .env; set +a`. In CI, .env is usually not committed (secrets). So the workflow will fail or run with empty vars unless secrets are set in GitHub. Document that for CI, Sigma/connection secrets must be configured in the repo’s GitHub Actions secrets and the workflow should source them (e.g. from env or a generated .env from secrets), or the workflow is "local conversion only" and doesn’t need .env in CI if it only runs the Python converter with public LookML.

---

## 7. Reduce dependencies

**Current dependencies:**

- **Python (requirements.txt):** pyyaml, lkml, requests.
- **Local setup path (setup.sh main loop):** Only runs `tools/build_sigma_explore_json.py` per project. That script imports only **lkml** (and stdlib). create_model.py needs **requests**.
- **Optional/CI path:** The workflow (and the embedded YAML in setup) runs `patch_semantic_models.py` and `generate_semantic_manifest.py`, which use **yaml** (pyyaml). So **pyyaml** is only required for the dbt/Node CI path and those two tools; it is **not** used by build_sigma_explore_json.py for the default local conversion.

**Reduction options:**

- **Document minimal vs full:** In README, state that for **local conversion + create_model only**, the minimal install is `lkml` and `requests`. **pyyaml** is needed only if you run `patch_semantic_models.py` or `generate_semantic_manifest.py` (e.g. for the optional dbt/Node CI workflow).
- **Optional:** Provide `requirements-minimal.txt` with only `lkml` and `requests` for users who never run the dbt path or the generated workflow; keep `requirements.txt` as full (add pyyaml) for CI and optional tools. Setup would use requirements.txt so CI and full local use still work.
- **Stop generating the heavy workflow:** If Section 5 chooses to remove or simplify the workflow so that setup no longer emits a workflow that uses dbt + Node + patch_semantic_models + generate_semantic_manifest, then **pyyaml** could be dropped from the default requirements.txt, and those two tools would be "optional" (document only). Then the only hard dependencies are **lkml** and **requests** for the core path.

**System/optional tools:**

- **jq:** Grep shows jq is not used in the repo scripts; no need to require it.
- **Node/npm/dbtc:** Only used inside the generated GitHub Actions workflow (clone dbt_semantics_to_sigma, npm install, dbtc). If the workflow is simplified to Python-only conversion, Node and dbtc are no longer dependencies of this repo for the default path.
- **git:** Only required if the user runs push_to_git.sh or uses the CI push step. If git automation is removed (Section 5 Option A), git is just "use it if you want version control."

**Summary:** Reduce dependencies by (1) documenting that the core path needs only lkml + requests; (2) making pyyaml optional unless the user runs patch/generate or the full CI workflow; (3) if the workflow is simplified to Python-only, dropping Node/dbtc from the repo’s dependency story.

---

## Suggested order of implementation

1. **1.1** — Env var mismatch (create_model.py accept API_* fallback).
2. **5** — Git: remove or simplify (push_to_git.sh, setup prompt, workflow commit/push).
3. **1.2, 1.4** — Pre-flight checks (setup.sh, regenerate_sigma_json.sh); **6** — missing .env handling in regenerate_sigma_json.sh.
4. **1.3** — build_sigma_explore_json.py validation and reporting.
5. **2.1** — Single env var reference (+ .env.example if desired).
6. **2.2, 2.3** — README Quick start and Troubleshooting; CONNECTIONS troubleshooting.
7. **1.5, 1.6** — create_lookml.sh hardening; create_model.py timeout.
8. **2.4, 3.x** — Script headers and inline comments.
9. **7** — Dependency documentation (and optional requirements-minimal.txt or simplified workflow).
10. **4** — Pinned requirements, CI note, optional tests.
