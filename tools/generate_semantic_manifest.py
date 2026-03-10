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
