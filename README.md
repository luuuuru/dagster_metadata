This repository contains a data orchestration and lineage tracking project built with **[Dagster](https://dagster.io/)**. It is designed to orchestrate analytical pipelines running on distributed databases (**Apache Impala**) using the **[Ibis](https://ibis-project.org/)** framework.

The primary objective is to enrich data assets with dynamic metadata, comprehensive data dictionaries, and column-level lineage. It achieves this by automating metadata extraction at both the table and column levels.

---

## Repository Structure

* **`defs/assets/`**: Contains the core data pipeline asset definitions.
* **`definitions.py`**: Serves as the main entry point for the Dagster code location, instantiating the `Definitions` object.
* **`defs/resources.py`**: Centralizes the database connection logic, ensuring all pipeline assets share a unified infrastructure configuration.
* **`column_schema.py`**: Defines reusable schema templates (`dg.TableSchema`) to populate structural metadata within the Dagster UI.

---

## References

* [Dagster Documentation](https://docs.dagster.io/)
