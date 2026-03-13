#!/usr/bin/env bash
# Import LookML from a zip file into lookml/<model_name>/ so the next conversion run picks it up.
# Usage: ./import_lookml.sh [path/to/file.zip]
#   - If no path given, uses the first .zip found in lookml_import/ (if that dir exists).
# Structure expected in the zip:
#   - Either: one top-level directory containing .lkml files (directory name = model name)
#   - Or: .lkml files at the root of the zip (model name = zip filename without .zip)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOOKML_DIR="${LOOKML_DIR:-lookml}"
IMPORT_DIR="${SCRIPT_DIR}/lookml_import"
TMP_DIR=""

cleanup() {
  [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ] && rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Resolve zip path
ZIP_PATH=""
if [ -n "${1:-}" ]; then
  ZIP_PATH="$(cd "$SCRIPT_DIR" && realpath "$1")
  [ ! -f "$ZIP_PATH" ] && echo "Error: Not a file: $1" >&2 && exit 1
else
  if [ ! -d "$IMPORT_DIR" ]; then
    echo "Usage: $0 [path/to/lookml.zip]"
    echo "  Or: put one or more .zip files in lookml_import/ and run $0 with no arguments."
    exit 1
  fi
  ZIP_PATH="$(find "$IMPORT_DIR" -maxdepth 1 -name "*.zip" -type f | head -n 1)"
  [ -z "$ZIP_PATH" ] && echo "Error: No .zip file found in $IMPORT_DIR" >&2 && exit 1
  echo "Using zip: $ZIP_PATH"
fi

# Unzip to temp dir
TMP_DIR="$(mktemp -d)"
unzip -q -o "$ZIP_PATH" -d "$TMP_DIR"

# Detect structure: one top-level dir with .lkml, or .lkml at root
TOP_ITEMS=("$TMP_DIR"/*)
if [ ${#TOP_ITEMS[@]} -eq 0 ]; then
  echo "Error: Zip is empty." >&2
  exit 1
fi

MODEL_NAME=""
SOURCE_DIR=""

if [ ${#TOP_ITEMS[@]} -eq 1 ] && [ -d "${TOP_ITEMS[0]}" ]; then
  # Single top-level directory
  CANDIDATE="${TOP_ITEMS[0]}"
  if [ -n "$(find "$CANDIDATE" -maxdepth 3 -name "*.lkml" -type f 2>/dev/null)" ]; then
    MODEL_NAME="$(basename "$CANDIDATE")"
    SOURCE_DIR="$CANDIDATE"
  fi
fi

if [ -z "$SOURCE_DIR" ]; then
  # Check for .lkml at root of zip
  if [ -n "$(find "$TMP_DIR" -maxdepth 1 -name "*.lkml" -type f 2>/dev/null)" ]; then
    MODEL_NAME="$(basename "$ZIP_PATH" .zip)"
    MODEL_NAME="${MODEL_NAME%.zip}"
    [ -z "$MODEL_NAME" ] && MODEL_NAME="imported"
    SOURCE_DIR="$TMP_DIR"
  fi
fi

if [ -z "$SOURCE_DIR" ] || [ -z "$MODEL_NAME" ]; then
  echo "Error: Zip must contain either (1) one top-level directory with .lkml files, or (2) .lkml files at the root." >&2
  exit 1
fi

# Sanitize model name (no path separators, no dots)
MODEL_NAME="$(echo "$MODEL_NAME" | tr -d './\\' | sed 's/^ *//;s/ *$//')"
[ -z "$MODEL_NAME" ] && MODEL_NAME="imported"

DEST_DIR="${SCRIPT_DIR}/${LOOKML_DIR}/${MODEL_NAME}"
mkdir -p "$DEST_DIR"
if [ -d "$DEST_DIR" ] && [ "$(ls -A "$DEST_DIR" 2>/dev/null)" ]; then
  echo "Warning: $DEST_DIR already has content. Copying over (existing files may be overwritten)."
fi
(cd "$SOURCE_DIR" && cp -R . "$DEST_DIR"/)
echo "Imported to $DEST_DIR (model name: $MODEL_NAME)."
echo "Re-run setup or the conversion loop to generate Sigma JSON for this model."
