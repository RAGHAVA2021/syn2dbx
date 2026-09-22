

The package has **two phases**: one-time setup and the nightly load. For your preferred approach, mapping discovery and profiling run on your manually created Databricks cluster; the nightly table loads run on serverless.

### One-time setup, in order

| Order | File                                                                                | Purpose                                                                                                                                                            |
| ----- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1     | `README.md`, `DESIGN_v9.md`, `IMPLEMENTATION_STATUS.md`, `WHERE_TO_ENTER_VALUES.md` | Read the design, configuration steps and outstanding production controls.                                                                                          |
| 2     | `config.example.json`                                                               | Starting template for connections, mappings, profile thresholds and concurrency limits.                                                                            |
| 3     | `databricks/generate_mappings_pyspark.py`                                           | Discover Synapse tables and all their columns from your manual Databricks cluster. Writes a draft config to a Unity Catalog volume.                                |
| 4     | `databricks/profile_inventory_pyspark.py`                                           | Profile those physical tables using Spark JDBC. Writes the row, column and size report and a new versioned config. **This is the profiler to use for your setup.** |
| 5     | `sql/01_azure_sql_control.sql`                                                      | Create the bridge control tables and source fence in Azure SQL Database.                                                                                           |
| 6     | `sql/02_synapse_setup.sql`                                                          | Create Synapse audit objects and the CETAS connection to ADLS Gen2.                                                                                                |
| 7     | `python/render_setup.py`                                                            | Generate the control-database seed SQL and Unity Catalog setup SQL from the approved config. The generated consumer views require physical tables to exist first.  |
| 8     | `databricks/load_candidate.py`                                                      | Deploy as a **serverless loader Job**; it will read staged Parquet and write managed Delta candidate or snapshot tables.                                           |
| 9     | `databricks/publish.py`                                                             | Deploy as separate **serverless publisher Jobs** for independent tables and release-pointer groups.                                                                |

If a table needs the ADF Copy route, also use `adf/bridge_synapse_source.dataset.json`, `adf/bridge_parquet_sink.dataset.json`, `adf/bridge_copy_to_parquet.pipeline.json`, and `python/deploy_adf.py` during setup.

### Nightly run, in order

| Order | File or action                                    | Purpose                                                                                                                                                    |
| ----- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1     | `python/fence.py write-start`                     | Reserve the source generation for Synapse writers.                                                                                                         |
| 2     | Existing Synapse jobs                             | Finish the normal table updates or materialisation.                                                                                                        |
| 3     | `python/upstream_audit.py`                        | Record source row counts and schema versions after the writes finish.                                                                                      |
| 4     | `python/fence.py write-close`, then `bridge-hold` | Close source writes and hold that generation stable for extraction.                                                                                        |
| 5     | `python/bridge_run.py`                            | Export tables to ADLS by CETAS or approved Copy route, then invoke `databricks/load_candidate.py`. It uses the profile classes to control concurrent work. |
| 6     | `python/fence.py bridge-release`                  | Release the source hold after the required source checks pass.                                                                                             |
| 7     | `python/publish_run.py`                           | Check publication evidence and invoke `databricks/publish.py` to expose validated data.                                                                    |

`python/generate_mappings.py` and `python/profile_inventory.py` are **orchestration-host alternatives** to the two Databricks notebooks; you do not need to run both versions. `python/source_metadata.py` is a helper for checking an individual schema hash. `requirements.txt` installs orchestration-host dependencies, and `tests/test_contract.py` contains local config checks.

**Important:** Profiling is a planning step, not a nightly replacement for `upstream_audit.py`. The profile estimates workload size; the nightly audit and reconciliation check the actual generation. The package still requires the production controls listed in `IMPLEMENTATION_STATUS.md` before a full 1,000-table rollout.


They run **one after the other** on your manually created Databricks cluster:

|                    | `generate_mappings_pyspark.py`                                      | `profile_inventory_pyspark.py`                                                        |
| ------------------ | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Main question      | **What tables and columns must we load?**                           | **How large are those tables, and how should we schedule them?**                      |
| Input              | Synapse schema and target catalog/schema settings                   | The reviewed `config.generated.json`                                                  |
| Reads from Synapse | Table names, column names, order and data types                     | Table storage and row statistics from Synapse metadata                                |
| Output             | `config.generated.json` and an exceptions report                    | `config.profiled.json` and a CSV profile report                                       |
| Adds               | One mapping per table, with all supported columns and a schema hash | Estimated rows, actual column count, physical size and `SMALL`/`MEDIUM`/`LARGE` class |
| Reads table data?  | No                                                                  | No                                                                                    |

**Run order:** generate mappings → review and fill config values → profile inventory → review sizes and concurrency settings → approve the new config version.

For example, the first notebook discovers that `presentation.orders` has 42 columns and maps to `analytics.gold.orders`. The second finds its estimated row count and Synapse storage size, then assigns a workload class. That class helps control how many exports run at once. The nightly audit still obtains the row count used for load reconciliation.
