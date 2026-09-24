# Synapse to Databricks bridge v9.5.8: package tree

Source: `synapse_databricks_bridge_v9_5_8.zip`. It contains 40 files in 6 folders.

```
bridge_v9_5_8_scripts/
├── README.md                          # Overview, nightly command order, failure/recovery, version changelog
├── SCRIPT_GUIDE.md                    # Every script in execution order: purpose, who runs it, reads/writes
├── DESIGN_v9.md                       # Governing design (140 KB)
├── IMPLEMENTATION_STATUS.md           # What is implemented vs. still needed per environment
├── WHERE_TO_ENTER_VALUES.md           # Where each config value, grant and job ID goes
├── config.example.json                # Template mapping inventory (copy to a private config.json)
├── requirements.txt                   # Runtime Python dependencies
├── requirements-dev.txt               # Test dependencies
│
├── sql/
│   ├── 01_azure_sql_control.sql       # New control DB: bridge.* tables + bridge.transition_fence
│   ├── 02_synapse_setup.sql           # Synapse dedicated pool: bridge_meta audit tables, credential,
│   │                                  #   external data source, Parquet file format (for CETAS)
│   └── 03_upgrade_to_v9_5.sql         # In-place upgrade of v9 to v9.5.5 control DBs (rerunnable)
│
├── python/                            # Runs on the control host (ODBC connection strings in env vars)
│   ├── bridge_run.py                  # Main orchestrator: extraction (CETAS/Copy) + Databricks loading; --resume
│   ├── publish_run.py                 # Gated publication to consumer tables/pointers; --resume
│   ├── fence.py                       # Source fence transitions (calls bridge.transition_fence), abandon/reopen
│   ├── recovery.py                    # Evidence-gated resolution of indeterminate attempts/publications
│   ├── upstream_audit.py              # Records upstream run/object audit while writer holds WRITE_ACTIVE
│   ├── render_setup.py                # Generates azure_sql_seed.sql + unity_catalog_setup.sql per wave
│   ├── generate_mappings.py           # Discovers a Synapse schema, writes a pinned mapping config
│   ├── profile_inventory.py           # DMV-based size profiling -> new inventory version + CSV report
│   ├── source_metadata.py             # Source schema hash (CLI + shared library)
│   ├── deploy_synapse_pipeline.py     # Optional: deploys the Copy pipeline and datasets
│   └── retire_releases.py             # Retention planning/execution (dry run by default)
│
├── databricks/                        # Notebooks run as Databricks Jobs or on a cluster
│   ├── load_candidate.py              # Loader Job: Parquet -> Delta candidate / snapshot slice
│   ├── publish.py                     # Strategy A publisher + group-pointer publisher (PUBLISH/INSPECT)
│   ├── retire_releases.py             # Retention Job: drops candidates, deletes staging
│   ├── generate_mappings_pyspark.py   # Cluster alternative to python/generate_mappings.py (UC JDBC)
│   └── profile_inventory_pyspark.py   # Cluster alternative to python/profile_inventory.py
│
├── synapse_pipeline/                  # Only needed for COPY_ACTIVITY mappings
│   ├── bridge_copy_to_parquet.pipeline.json
│   ├── bridge_synapse_source.dataset.json
│   └── bridge_parquet_sink.dataset.json
│
├── tests/                             # pytest; SQLite-backed simulations, no live Azure needed
│   ├── test_contract.py               # Config, manifest, path and naming contract checks
│   ├── test_orchestration_sim.py      # End-to-end bridge_run/publish_run against SQLite stand-ins
│   ├── test_static.py                 # AST checks: no rebound functions; PACKAGE_DIR matches folder name
│   ├── test_v94_regressions.py
│   ├── test_v95_regressions.py
│   ├── test_v952_regressions.py
│   ├── test_v955_regressions.py
│   ├── test_v956_regressions.py
│   ├── test_v957_regressions.py
│   └── test_v958_regressions.py
│
└── generated/                         # (not in zip; created by render_setup.py --output-dir generated)
    ├── azure_sql_seed.sql             # Run in Azure SQL control DB
    └── unity_catalog_setup.sql        # Run on a Databricks SQL warehouse
```

## File counts

| Folder | Files |
|---|---|
| (root) | 8 |
| `sql/` | 3 |
| `python/` | 11 |
| `databricks/` | 5 |
| `synapse_pipeline/` | 3 |
| `tests/` | 10 |
| **Total** | **40** |

## Notes

- `generated/` is not part of the package. `python/render_setup.py` creates it for each inventory wave.
- `python/__pycache__/` also appears after any script has been run. It is not part of the package.
- The folder name `bridge_v9_5_8_scripts` must match `PACKAGE_DIR` in `databricks/generate_mappings_pyspark.py` (`/Workspace/Shared/bridge_v9_5_8_scripts`). `tests/test_static.py` enforces this.
