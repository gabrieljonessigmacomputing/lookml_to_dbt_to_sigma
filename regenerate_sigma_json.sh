#!/usr/bin/env bash
# Regenerate all sigma_model/*.json from LookML. Run this after changing LookML or
# converter logic; the files are only updated when this (or setup.sh) runs.
# create_model.py only reads existing JSON and does not regenerate it.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
set -a
# shellcheck source=/dev/null
source .env
set +a
# Dirs: only create (mkdir -p); never remove or replace. Files: overwrite.
mkdir -p out sigma_model
for dir in looker_files/*/; do
  [ ! -d "$dir" ] && continue
  dir="${dir%/}"
  model_name="$(basename "$dir")"
  echo "--- Processing: $model_name ---"
  mkdir -p "sigma_model/$model_name"
  python3 tools/build_sigma_explore_json.py "$dir" "sigma_model/$model_name" "$model_name"
done
echo "Done. Sigma JSON is in sigma_model/ (run create_model.py to push to Sigma)."
