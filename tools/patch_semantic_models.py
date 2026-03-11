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
