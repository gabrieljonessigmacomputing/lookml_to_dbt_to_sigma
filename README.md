# LookML → dbt Semantic Layer → Sigma (TEST mode)

This repo is a starter project to translate LookML modeling into Sigma Data Models
via the dbt Semantic Layer, using only:

- LookML files in `lookml/`
- A simple `.env` configuration
- A GitHub Actions workflow

No dbt profile or warehouse credentials are required. The pipeline runs in **TEST mode**,
so it does **not** make real Sigma API calls by default.
