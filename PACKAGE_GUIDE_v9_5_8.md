# Synapse → Databricks bridge v9.5.8: what each file does, in run order, and where it runs

This guide covers the 40 files in `bridge_v9_5_8_scripts`. It explains them in the order you use them and says, for each one, **where it runs**. For the exact values and grants to enter, see `WHERE_TO_ENTER_VALUES.md`; for the command lines, see `README.md`.

---

## 1. The four places where things run

Most of the work does not run "on Synapse" or "on Databricks" directly. It is started from one orchestration machine, which then tells Synapse and Databricks what to do.

| Place | What it is | What runs there |
|---|---|---|
| **Control host** | Your orchestration machine: a VM, container or scheduler agent with Python 3.9 or later (the code uses 3.9 features; the package's tests pass on 3.12), Microsoft ODBC Driver 18 and `pip install -r requirements.txt`. Your existing upstream scheduler calls it. | **Every script in `python/`.** These scripts connect to Synapse and Databricks rather than running inside them. The one exception is the optional Step 0a notebook, which imports `python/generate_mappings.py` and `python/source_metadata.py` as libraries on a Databricks cluster. |
| **Azure SQL control database** | A dedicated Azure SQL Database that stores the bridge's state: the source fence, runs, attempts, targets and recovery events. | `sql/01_azure_sql_control.sql` or `sql/03_upgrade_to_v9_5.sql`, and the generated `azure_sql_seed.sql`. Run them in a SQL editor (SSMS, Azure Data Studio or the portal query editor). |
| **Synapse** (dedicated SQL pool and workspace) | Your source system. | `sql/02_synapse_setup.sql`, run once in the dedicated pool. During each run, the **CETAS** statements (sent by the control host) and the optional **Copy pipeline** (`synapse_pipeline/*.json`) execute inside Synapse and write Parquet to ADLS. |
| **Databricks** | The target system (Unity Catalog, serverless Jobs, SQL warehouse). | The notebooks in `databricks/`: three become the four **Jobs** (loader and two publishers every night, retention periodically), and two are optional one-time notebooks. The generated `unity_catalog_setup.sql` runs on a **SQL warehouse**. |

ADLS Gen2 sits in between. Synapse writes Parquet there and Databricks reads it. The control host checks that each new attempt folder is empty, uploads the job manifests that the Databricks Jobs read, and (with `retire_releases.py --delete-staging`) deletes old staging folders.

### How one nightly run moves between the places

```text
Control host                    Azure SQL          Synapse                    ADLS              Databricks
------------                    ---------          -------                    ----              ----------
fence.py write-start  ────────► fence row
[your Synapse transformations run in Synapse]
upstream_audit.py  ─────────────────────────────► counts + audit rows
fence.py write-close / bridge-hold ► fence row
bridge_run.py  ────────────────► run/attempt rows
               ─ CETAS / Copy ──────────────────► reads views ──► Parquet
               ─ loader Job ─────────────────────────────────────────────────────► load_candidate.py
                                                                   Parquet ──────► Delta tables
fence.py bridge-release ───────► fence row
publish_run.py ────────────────► target rows
               ─ publisher Job ──────────────────────────────────────────────────► publish.py
retire_releases.py (periodic) ─► attempt rows
               ─ retention Job ──────────────────────────────────────────────────► retire_releases.py
                                                                                   (old snapshot rows,
                                                                                    candidate tables)
               ─ --delete-staging ────────────────────────────────► old Parquet
```

### Connections the control host needs

| Setting | Used for | Used by |
|---|---|---|
| `BRIDGE_CONTROL_ODBC` | Azure SQL control database | `fence.py`, `upstream_audit.py`, `bridge_run.py`, `publish_run.py`, `recovery.py`, `retire_releases.py` |
| `BRIDGE_SYNAPSE_AUDIT_ODBC` | Synapse, read metadata and write the `bridge_meta` audit | `generate_mappings.py`, `profile_inventory.py`, `source_metadata.py`, `upstream_audit.py`, `fence.py bridge-hold`, `bridge_run.py` |
| `BRIDGE_CETAS_ODBC` | Synapse, a separate identity that runs CETAS | `bridge_run.py` |
| Azure sign-in (`DefaultAzureCredential`) | ADLS staging and the Synapse pipeline API | `bridge_run.py`, `publish_run.py`, `retire_releases.py`, `deploy_synapse_pipeline.py` |
| Databricks authentication (unified auth) | Starting and polling the Jobs | `bridge_run.py`, `publish_run.py`, `retire_releases.py` |

---

## 2. Folder map

| Folder | Contains | Runs where |
|---|---|---|
| `sql/` | One-time database setup scripts | Azure SQL (01, 03) and the Synapse dedicated pool (02) |
| `python/` | The command-line scripts that drive everything | Control host |
| `databricks/` | Notebooks | Databricks: three as Jobs, two as optional one-time notebooks |
| `synapse_pipeline/` | Definitions of the optional Copy pipeline and its two datasets | Deployed into the Synapse workspace; executes in Synapse |
| `tests/` | Automated tests | A developer machine or CI only, never production |
| *(package root)* | Config example, dependency lists and documentation | Read by people; `config.json` is read by the control host |

---

## 3. Step by step, in the order you use them

Step numbers follow `SCRIPT_GUIDE.md` and the header comment at the top of each script, with two differences: this guide splits Step 1 into 1a (Azure SQL) and 1b (Synapse), which both SQL headers call "Step 1", and the recovery steps R1 to R4 are numbered only in `SCRIPT_GUIDE.md`. `sql/03_upgrade_to_v9_5.sql` and `python/recovery.py` carry no step number.

### Phase A: build the inventory (once per wave of tables)

**Step 0a: `python/generate_mappings.py`**
- **Runs on:** control host, connecting to Synapse.
- **Does:** reads the tables or views of one Synapse schema and writes a new config with one mapping per object, including columns in order, types and a schema hash. If any column needs manual review, it writes no config and lists the problems in `*.exceptions.json`.
- **Alternative:** `databricks/generate_mappings_pyspark.py`, run once as a notebook on a Databricks classic cluster (or serverless, if you use a Unity Catalog JDBC connection) that can reach Synapse. Upload the whole `bridge_v9_5_8_scripts` folder as Workspace files first, because the notebook imports `python/generate_mappings.py` from it via `PACKAGE_DIR`. It writes the same config to a Unity Catalog volume; copy the reviewed file to the control host. Use one or the other.

**Step 0b: `python/profile_inventory.py`** (optional)
- **Runs on:** control host, connecting to Synapse.
- **Does:** reads Synapse size statistics (no table data), classifies each mapping as SMALL, MEDIUM or LARGE, and writes a new inventory version plus a `.profile.csv` report. Without this step, every mapping is treated as MEDIUM.
- **Before running:** add a top-level `profile_thresholds` block to the input config by hand. `config.example.json` does not contain one (although `WHERE_TO_ENTER_VALUES.md` says it does), and Step 0a copies that file as its template, so a generated config never has it. Without it both profilers stop with "Set reviewed increasing profile_thresholds in the input config". All six keys are required, each a positive integer, and each `medium_*` value must be lower than its `large_*` value. A mapping is LARGE if any measure reaches its `large_*` value, otherwise MEDIUM if any reaches its `medium_*` value, otherwise SMALL. The numbers below only show the shape; choose your own:

  ```json
  "profile_thresholds": {
    "medium_rows": 10000000, "large_rows": 100000000,
    "medium_bytes": 5368709120, "large_bytes": 53687091200,
    "medium_columns": 100, "large_columns": 300
  }
  ```
- **Alternative:** `databricks/profile_inventory_pyspark.py` as a notebook on a Unity Catalog-enabled classic cluster. It reads and writes the config in a Unity Catalog volume; copy the result to the control host.

**Step 0c: `python/source_metadata.py`**
- **Runs on:** control host, connecting to Synapse.
- **Does:** prints the schema hash of one view, for mappings you write or edit by hand. It is also the shared library that `upstream_audit.py` and `bridge_run.py` use to detect schema changes.

**Then:** review the config, choose `REPLACE_TABLE` or `RELEASE_POINTER` for each mapping, fill in every `<...>` value (including `staging_abfss_root` and `control_catalog`) and save it as `config.json` on the control host. The Job IDs are added in Step 2c. The example config also leaves out the optional tuning settings, which then take their defaults: `max_extraction_workers` (4), `extraction_class_limits` (SMALL 4, MEDIUM 2, LARGE 1), `max_loader_workers` (1), `max_publisher_workers` (1), `max_attempts` (3), `takeover_after_minutes` (150, minimum 135), `heartbeat_seconds` (60) and `minimum_fence_margin_minutes` (30). Add any you want to change; `WHERE_TO_ENTER_VALUES.md` explains them.

### Phase B: set up the platform (once, plus Step 2 for every new wave)

**Step 1a: `sql/01_azure_sql_control.sql`**
- **Runs in:** the Azure SQL control database, as its administrator.
- **Does:** creates the `bridge` schema: the fence, fence events, runs, attempts, group members, target state and recovery events tables, plus the `bridge.transition_fence` procedure. Afterwards, insert the one `bridge.source_fence` seed row and grant the orchestration identity its rights, including `SELECT` on `bridge.fence_events`.
- **Existing deployments:** a v9 through v9.5.5 database runs `sql/03_upgrade_to_v9_5.sql` instead of 01. A v9.5.6 or v9.5.7 database needs no Azure SQL schema change. Either way, upgrading to v9.5.8 also needs these steps (see "v9.5.8 review fixes" in `README.md`):
  - If the orchestration identity has table-by-table rights, grant it `SELECT` on `bridge.fence_events`; `publish_run.py` now reads it.
  - Rerun `generated/unity_catalog_setup.sql` once (Step 2), so the existing `published_groups` table gets deletion vectors. This upgrades its Delta protocol: any reader of it, or of the pointer views, outside Databricks must support deletion vectors.
  - Redeploy the three changed notebooks: `load_candidate.py` (loader Job), `publish.py` (both publisher Jobs) and `retire_releases.py` (retention Job).
  - The loader now always runs in UTC. If your workspace uses another default session time zone, check once any mapping that converts `timestamp` to `timestamp_ntz` or `string`.

**Step 1b: `sql/02_synapse_setup.sql`**
- **Runs in:** the Synapse dedicated SQL pool, as its deployment identity (for example in Synapse Studio). An administrator must first create the database master key.
- **Does:** creates the `bridge_meta` schema, the managed-identity credential, the ADLS external data source and Parquet file format used by CETAS, and the two audit tables `upstream_run` and `upstream_object`. Replace the ADLS container and account in `LOCATION` before running it.

**Step 2: `python/render_setup.py`**
- **Runs on:** control host. It needs no connections; it only writes two files.
- **Does:** turns `config.json` into two idempotent SQL files, which you then run in two different places:
  - `generated/azure_sql_seed.sql`: run in the **Azure SQL** control database. It seeds group members and each target's initial state.
  - `generated/unity_catalog_setup.sql`: run on a **Databricks SQL warehouse** as the deployment identity. It creates the schemas and the `published_groups` pointer table, and for `RELEASE_POINTER` mappings the `_snapshots` tables and consumer views. `REPLACE_TABLE` consumer tables are created by their first publication.
- **Repeat:** for every new inventory wave. It never resets live state. When upgrading an existing deployment to v9.5.8, rerun the Unity Catalog file once even without a new wave (see Step 1a).

**Step 2b: `python/deploy_synapse_pipeline.py` with `synapse_pipeline/*.json`** (only for `COPY_ACTIVITY` mappings)
- **Runs on:** control host. It uploads the definitions into the **Synapse workspace**, where the pipeline later executes.
- **Files:**
  - `bridge_synapse_source.dataset.json`: the Synapse source dataset.
  - `bridge_parquet_sink.dataset.json`: the ADLS Parquet sink dataset.
  - `bridge_copy_to_parquet.pipeline.json`: the Copy pipeline.
- **Before running:** the Synapse and ADLS linked services must already exist. CETAS-only deployments skip this step.

**Step 2c: create the Databricks Jobs**
- **Done in:** the Databricks workspace UI.
- **Does:** import three notebooks and create four serverless, single-task Jobs, each with its own Run-as service principal. Put the four Job IDs in `config.json`.

| Notebook | Job | Config key |
|---|---|---|
| `databricks/load_candidate.py` | Loader | `loader_job_id` |
| `databricks/publish.py` | Table publisher (`REPLACE_TABLE`) | `publisher_job_id` |
| `databricks/publish.py` | Group-pointer publisher (`RELEASE_POINTER`) | `pointer_job_id` |
| `databricks/retire_releases.py` | Retention | `retention_job_id` |

Each Job takes the notebook parameters `manifest_path` and `manifest_sha256` with empty defaults; the control host fills them on every run. Also on the Databricks side: create the ADLS staging external location and grant `READ FILES` on it to each Job identity. Disable serverless auto-optimization on all four Jobs. Set a Job timeout of at most 7,200 seconds on the loader and publisher Jobs, and set their maximum concurrent runs to at least `max_loader_workers` and `max_publisher_workers`. `WHERE_TO_ENTER_VALUES.md` has the exact settings.

### Phase C: the nightly run

Your existing scheduler calls these on the control host in this order, with the same generation ID throughout.

| Step | File and command | Runs on | What happens where |
|---|---|---|---|
| 3 | `python/fence.py write-start` | Control host | Azure SQL: the upstream writer claims the fence before changing any Synapse table. |
| — | *Your existing Synapse transformations* | Synapse | Not part of this package; they run while the writer holds the fence. |
| 4 | `python/upstream_audit.py` | Control host | Synapse: counts each mapped view and writes the audit rows to `bridge_meta`. Azure SQL: confirms the writer still holds the fence. |
| 5 | `python/fence.py write-close`, then `python/fence.py bridge-hold` | Control host | Azure SQL: closes the writer's ownership, then hands the frozen generation to one bridge run. `bridge-hold` also checks the Synapse audit is complete. |
| 6 | `python/bridge_run.py` | Control host | **Synapse** runs CETAS (or the Copy pipeline) and writes Parquet to ADLS. **Databricks** runs the loader Job (`load_candidate.py`), which loads the Parquet into Delta. **Azure SQL** records every attempt. The run ends `READY`; nothing is published yet. |
| — | *Your independent source checks* | Your monitoring tooling | Produce `fence_evidence.json` for this generation: all source requests terminal, no source retry needed, audit healthy, no fence violation, smoke check passed, each with an evidence URI. |
| 7 | `python/fence.py bridge-release --evidence fence_evidence.json` | Control host | Azure SQL: returns the source to writers. It needs that evidence file, a `READY` run and no running or indeterminate source read. |
| — | *Your policy and consumer tests* | Your test tooling | Produce `publication_evidence.json` from real test results. |
| 8 | `python/publish_run.py` | Control host | **Databricks** runs the publisher Jobs (`publish.py`), which make the release visible to consumers. **Azure SQL** records publication. It refuses to start until Step 7 is recorded. |

### Phase D: maintenance and recovery (when needed)

| Step | File | Runs on | Purpose |
|---|---|---|---|
| 9 | `python/retire_releases.py` | Control host, which starts the **Databricks** retention Job (`databricks/retire_releases.py`) and, with `--delete-staging`, deletes ADLS staging itself | Weekly or similar. The Job removes release rows from `_snapshots` tables that are no longer kept, and drops old candidate tables. Candidate tables and staging folders also wait for the retention windows: `retention_days_published` (default 7) days after publication, `retention_days_unpublished` (default 30) days after the run's last state change otherwise. It is a dry run unless you pass `--execute`. |
| R1 | `python/bridge_run.py ... --resume` | Control host | Continues a failed extraction or load run with the same arguments. |
| R2 | `python/publish_run.py ... --resume` | Control host | Continues a run left in `PUBLISHING`, checking the actual Delta state first. |
| R3 | `python/fence.py abandon`, then `python/fence.py reopen` | Control host → Azure SQL | Gives up on an unusable generation and frees the fence for the next one. |
| R4 | `python/recovery.py` | Control host → Azure SQL | Records evidence that an operation with an unknown outcome has finished, so a run can continue or be abandoned. |

---

## 4. Supporting files

| File | Purpose | Used where |
|---|---|---|
| `config.example.json` | Example inventory: one `REPLACE_TABLE` mapping and a two-member `RELEASE_POINTER` group, plus the required top-level settings, the four Job IDs, the retention windows and `synapse_pipeline`. It does not contain `profile_thresholds` or the optional tuning settings (see Step 0b and the end of Phase A). Copy it to `config.json`. | Control host (as `config.json`) |
| `requirements.txt` | Python packages for the control host. | Control host |
| `requirements-dev.txt` | Adds `pytest` for running the tests. | Developer machine or CI |
| `README.md` | Deployment steps, the full nightly command sequence, recovery rules, evidence file formats and the change history, including v9.5.8. | Read by people |
| `SCRIPT_GUIDE.md` | Detailed description of each script in execution order. | Read by people |
| `WHERE_TO_ENTER_VALUES.md` | Every environment value, config field, Job setting and grant. | Read by people |
| `IMPLEMENTATION_STATUS.md` | What is implemented and what still needs environment-specific work before production. | Read by people |
| `DESIGN_v9.md` | The governing design the code implements. | Read by people |

### Tests (`tests/`, developer machine or CI only)

Run all of them with `pytest -q tests`. They use SQLite stand-ins and fakes, so no Azure or Databricks connection is needed; they do not replace a platform acceptance test.

| File | Covers |
|---|---|
| `test_contract.py` | Configuration rules, naming, paths and Synapse Copy row-count checks. |
| `test_static.py` | No script overwrites its own functions, and `PACKAGE_DIR` matches the folder name. |
| `test_orchestration_sim.py` | End-to-end runs of `bridge_run.py` and `publish_run.py`, including failure, resume, lease takeover and publication races. Also provides the shared simulation that six of the regression files reuse. |
| `test_v94_regressions.py` to `test_v958_regressions.py` (seven files) | One file per version's fixes, so each fixed bug stays fixed. `test_v958_regressions.py` covers the v9.5.8 changes. |

---

## 5. At a glance: every file and where it runs

| Where | Files |
|---|---|
| **Control host** | All 11 files in `python/` (the Step 0a notebook also imports `generate_mappings.py` and `source_metadata.py` on Databricks): `generate_mappings.py`, `profile_inventory.py`, `source_metadata.py`, `render_setup.py`, `deploy_synapse_pipeline.py`, `fence.py`, `upstream_audit.py`, `bridge_run.py`, `publish_run.py`, `retire_releases.py`, `recovery.py` |
| **Azure SQL control database** | `sql/01_azure_sql_control.sql` (new) or `sql/03_upgrade_to_v9_5.sql` (upgrade); generated `azure_sql_seed.sql` |
| **Synapse dedicated SQL pool** | `sql/02_synapse_setup.sql`; the CETAS statements sent by `bridge_run.py` |
| **Synapse workspace** | `synapse_pipeline/*.json` (three files; only for `COPY_ACTIVITY`) |
| **Databricks Jobs (serverless, started by the control host)** | Three Jobs every night: `databricks/load_candidate.py` (loader) and `databricks/publish.py` (two publisher Jobs); `databricks/retire_releases.py` (retention Job) when retention runs |
| **Databricks notebooks (optional, one-time)** | `databricks/generate_mappings_pyspark.py`, `databricks/profile_inventory_pyspark.py` |
| **Databricks SQL warehouse** | Generated `unity_catalog_setup.sql`; the external location SQL in `WHERE_TO_ENTER_VALUES.md` |
| **Developer machine or CI** | The 10 files in `tests/`, `requirements-dev.txt` |
| **Documentation** | `README.md`, `SCRIPT_GUIDE.md`, `WHERE_TO_ENTER_VALUES.md`, `IMPLEMENTATION_STATUS.md`, `DESIGN_v9.md` |
