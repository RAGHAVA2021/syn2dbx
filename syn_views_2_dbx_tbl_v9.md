# Synapse Materialised Views to Databricks Delta Tables

> Version 9 — implementation baseline with corrected publication privileges and group completeness.

## Document control

| Field | Value |
|---|---|
| Document owner | Azure Data Platform Owner — assign named person before approval |
| Technical approvers | Synapse Platform Owner; Databricks Platform Owner; Security/Governance Owner; Network/Cloud Platform Owner |
| Operational approver | Data Operations Owner |
| Status | Proposed implementation baseline |
| Verified against product documentation | 2026-09-09 Australia/Sydney business date (2026-09-08 UTC); revalidate preview and version-sensitive features before production deployment |
| Bridge exit criterion | Retire only after every in-scope consumer has moved to the permanent Databricks ingestion/transformation path, reconciliation has passed for the agreed parallel-run period, downstream owners have approved cutover, and rollback to the bridge is no longer required |

### Open production-verification gates

These are fail-closed deployment tests, not assumptions on which the design may rely. Record the tester, exact platform/runtime version, date, evidence location and result for each item before the affected feature or mapping is enabled:

- Verify effective governed Delta properties and table features before and after `CREATE OR REPLACE TABLE`. Publication always restates the approved user-managed property contract regardless of whether the tested runtime happens to retain unspecified properties.
- Verify the Databricks Runtime 18 LTS `TIME(p)` Beta enablement, source precision `0` through `7` to target precision `0` through `6`, selected loss-of-precision rule, Parquet round trip and every affected downstream consumer. Until this passes, use the approved non-preview representation.
- Verify the exact `SHOW POLICIES`, `SHOW EFFECTIVE POLICIES`, `DESCRIBE POLICY` and information-schema commands on the selected compute using the read-only bridge identity and confirm that `READ METADATA` provides the required visibility without policy-management authority.
- Verify Synapse SQL auditing scope, destination, latency, retention and alert delivery for DML/DDL against governed materialised tables; verify monitoring detects loss or deletion of the audit/diagnostic configuration. Auditing is a detection aid, not proof that every event will be captured.

### Requirement levels and delivery phases

- **MUST** means required before the first production publication.
- **SHOULD** means required unless a named approver records a justified exception.
- **PHASE 2** means an enhancement that may follow initial production only when the phase-one scope does not need it.

All requirements stated as commands in §§1–19 are **MUST** unless explicitly marked **SHOULD** or **PHASE 2**. Optional chunked extraction is conditional and is not a prerequisite for every table.

## 1. Decide publication consistency before implementation

Do not treat the enabled inventory automatically as either completely independent tables or one global transaction. Determine which tables must show the same upstream snapshot when users join them.

### Consistency-group assignment procedure

The Data Architecture Owner owns the initial assignment. Each affected business-data owner and downstream-application owner must approve the proposed grouping before production. Reassess the assignment whenever source lineage, consumer queries or publication requirements change.

For every mapping:

1. Extract direct and transitive lineage from the Synapse presentation view and materialised table into downstream Unity Catalog views, Power BI semantic models, reports, notebooks, jobs, APIs and data shares.
2. Analyse an approved lookback period of Synapse, Databricks SQL and reporting-tool query/refresh history to identify tables read or joined in the same logical operation. Where query telemetry is incomplete, inspect semantic-model relationships and refresh groups.
3. Interview the named business-data owner and downstream owner to identify reconciliation, period-close, regulatory and operational workflows that require one source generation.
4. Record every dependency edge, evidence source, owner, required consistency level and rationale in a review table. Do not infer independence merely because no query was observed.
5. Form the smallest groups that satisfy confirmed join and workflow requirements. Compute the transitive closure: if A must be consistent with B and B with C, assign A, B and C to the same group unless a documented release-pinned consumer contract proves otherwise.
6. Default an unreviewed table to a non-publishable `UNASSIGNED` state. It must not default silently to independent replacement. A named owner may approve `INDEPENDENT` only after lineage, telemetry and consumer review are complete.
7. Assess the proposed group's publication blast radius: enabled-member count, total data volume, p95 critical path including retries, recent member failure rate, probability that one failure blocks the group, number/criticality of affected consumers and maximum tolerable stale-release duration. Compare these values with organisation-approved warning and escalation thresholds. Do not impose an arbitrary universal table-count limit, because ten very-large critical tables can present more risk than a larger set of small reliable tables.
8. Require the business-data owner, downstream owner and Data Operations Owner to accept explicitly that one failed enabled member keeps the whole group on its previous release. A group exceeding any blast-radius threshold also requires Data Architecture Owner approval and a recorded decision to accept it, split it through a release-pinned interface that preserves the required consistency, or adopt another approved publication design. Group size or availability pressure must never be resolved by silently weakening a confirmed consistency requirement.
9. Validate each proposed group through representative joins, multi-query report refreshes and failure/rollback simulations. Store the approved group version in control metadata and bind every bridge release to that immutable version.

At minimum, the assignment evidence must contain `mapping_object_id`, proposed group, group-contract version, lineage references, observed joint consumers, required consistency level, blast-radius measurements/threshold results, availability acceptance, owner decisions, review date and approval status. A group change is a governed contract change: stop affected publication, recalculate readiness and do not combine members assessed under different group-contract versions.

### Staged mapping enablement

The review does not have to finish for the complete discovered inventory before the first approved wave publishes. Stage delivery without weakening the fail-closed default:

1. Create every discovered mapping with `enabled_flag = false` and consistency status `UNASSIGNED`.
2. Define a wave from reviewed mappings and their complete consistency groups. Never enable only part of a group or omit an upstream/downstream dependency required by that group's approved contract.
3. Complete source/target identity, lineage, extraction compatibility, column/schema, Delta-property, security, consumer, validation and blast-radius approvals for every mapping in the wave.
4. Record a consumer-serving decision, named owner, reason and target review/remediation date for each mapping left disabled. Existing consumers continue through the explicitly approved Synapse or other interim path; disabled objects must not disappear silently.
5. Change `enabled_flag` only through a conditional, audited deployment transition that requires the approved group-contract version and all required contract versions. The bridge precheck rejects an enabled `UNASSIGNED` mapping or a partially enabled consistency group.
6. Recalculate run expectations, capacity, fence budget and reconciliation counts for the enabled inventory version. Bind every bridge run to that immutable inventory version so a mid-run enablement change cannot alter its scope.

Each wave must pass the same production gates as the final estate. Enabling later waves does not permit existing approved groups to change membership without a new group-contract version and renewed blast-radius approval.

Every enabled mapping using `RELEASE_POINTER` must have `mandatory_flag = true`. Reject a group containing an enabled optional member at inventory approval and again at the bridge precheck. A mapping that is not required for the group must be disabled with its own consumer-serving route, or placed in a genuinely independent publication contract; it must not point to a group release that lacks its slice.

Use the following publication rule:

| Requirement | Publication method |
|---|---|
| Table is independent and a temporary mixed state across unrelated tables is acceptable | `CREATE OR REPLACE TABLE` |
| Several tables must become visible together | Release pointer keyed by `consistency_group` |
| Every enabled table must represent exactly one common snapshot | One global release pointer |

`CREATE OR REPLACE TABLE` is atomic for one Delta table and preserves the table's identity, history, granted privileges, row filters and same-named column masks. Preservation does not guarantee that a retained policy remains compatible after a column removal, rename or incompatible type change; every replacement is subject to the consumer contract and policy gate in §13. It is not atomic across multiple tables.

A release pointer allows a whole consistency group to move from the previous release to the new release by changing one control row. The consequence is deliberate: consistency is guaranteed within a group, not between independently published groups.

That guarantee applies to each query that resolves the pointer. It is not a transaction spanning every query issued during a multi-query Power BI refresh or application workflow. A consumer that requires one release across multiple queries must resolve the approved `release_id` once and use a release-pinned interface, or execute during an agreed publication quiet window and accept that the window is an operational rather than transactional control. Test the selected pattern with each affected reporting tool.

Do not replace a consistency group sequentially when strict collection-level consistency is required. Sequential replacement creates a temporary mixed-release state.

## 2. One-time platform setup

### Synapse objects

Maintain:

- One materialised table for each enabled transformed output.
- One simple presentation view over each materialised table.
- One upstream run audit table.
- One Parquet external file format.
- One ADLS Hadoop external data source backed by a database-scoped credential. For CETAS from a dedicated SQL pool, define it with `TYPE = HADOOP`; native external tables cannot be CETAS export targets in a dedicated SQL pool.
- A dedicated schema for temporary CETAS external-table metadata.

Before creating the database-scoped credential, the privileged Synapse deployment process must verify that the dedicated-pool user database contains a database master key. If it does not, an administrator with the required database control creates it once with an organisation-approved strong password and records its custody, recovery and rotation procedure. Never embed the master-key password in pipeline code, mapping metadata, notebooks or logs. The runtime CETAS identity does not create, open or alter the master key.

Dedicated SQL pool stores only authoritative upstream materialisation facts and Synapse request telemetry. It is not the default lease or bridge-orchestration store because its `PRIMARY KEY` and `UNIQUE` constraints are not enforced and it is not designed for high-frequency row-level coordination.

### Control-state authority

Use these authoritative stores:

| State | Authoritative store | Reason |
|---|---|---|
| Upstream run and per-materialised-table completion facts | Synapse dedicated SQL pool | Written and closed by the existing upstream process |
| Mapping, column contracts, compatibility history, bridge runs, object attempts, extraction chunks, leases, approvals and cleanup dispositions | Azure SQL Database | Enforced primary/unique keys, efficient conditional updates and transactional lease control |
| Current/previous release pointer and publication history used by Databricks consumers | Unity Catalog Delta control schema | Directly and transactionally queryable by stable Databricks consumer views |

The Azure SQL control database must enforce unique constraints for target identifiers, source identifiers, contract ordinals, release/object attempts and active lease keys. State transitions must be conditional updates from an expected prior state and control version. The orchestration identity, schema owner, DDL deployment identity, backup/restore policy and retention period must be named before production.

There is intentionally no cross-store transaction across Synapse, Azure SQL and Delta. Recovery always reads the authoritative source facts, Azure SQL object attempt, exact ADLS path, Delta history/embedded release metadata and publication pointer before advancing. An Azure SQL record alone is never proof that a Delta commit did or did not occur.

If Azure SQL Database cannot be introduced for phase one, a documented exception may retain operational rows in Synapse only with a single active orchestrator, application-enforced uniqueness checks before and after every insert, conditional state transitions, duplicate-detection alarms and a tested takeover procedure. This is a constrained fallback, not the preferred production design.

### Networking and regional deployment

Before implementation, record the region and network mode of the Synapse workspace/dedicated pool, ADLS Gen2 staging account, Azure Databricks workspace, Unity Catalog metastore and Azure SQL control database.

- The Unity Catalog metastore must be in a region supported by and assigned to the Databricks workspace; use same-region managed storage where required by the platform.
- The staging account must be ADLS Gen2 with hierarchical namespace enabled.
- Prefer Synapse, staging storage, Databricks and the control database in the same Azure region to reduce latency, cross-region transfer cost and failure domains. A cross-region exception requires measured throughput, cost and disaster-recovery approval.
- Record whether public endpoints, selected-network firewalls, network security perimeter or private endpoints are used. Private endpoints are mandatory only when organisational policy or the selected firewall mode requires them.
- Prove name resolution and routes from Synapse to ADLS, from the selected Databricks classic/serverless compute plane to ADLS, and from the orchestrator to Synapse, Azure SQL and Databricks APIs.
- Configure storage firewall/private connectivity for both the Synapse credential identity that writes CETAS files and the Databricks Access Connector identity/compute path that reads them. Identity RBAC without a permitted network path is insufficient.
- Record private DNS ownership, endpoint approval, outbound restrictions, trusted-service exceptions, TLS requirements and the exact connectivity tests used for deployment acceptance.

### Mapping table

The mapping table should contain at least:

| Column | Purpose |
|---|---|
| `mapping_object_id` | Immutable identifier used in audit keys, candidate names and storage paths; do not derive identity from an unqualified table name |
| `source_schema` | Synapse presentation-view schema |
| `source_view` | Presentation view to export |
| `materialised_table` | Underlying one-to-one Synapse table |
| `target_catalog` | Unity Catalog target catalog |
| `target_schema` | Unity Catalog target schema |
| `target_table` | Consumer Delta table |
| `consistency_group` | Tables that must publish from the same release |
| `consistency_status` | `UNASSIGNED`, `INDEPENDENT` or `GROUPED`; enabled mappings cannot remain `UNASSIGNED` |
| `group_contract_version` | Immutable approved consistency-assignment and blast-radius contract version |
| `publication_strategy` | `REPLACE_TABLE` or `RELEASE_POINTER` |
| `security_enforcement_mode` | For `RELEASE_POINTER`: `PHYSICAL_TABLE_POLICY` or `DYNAMIC_VIEW_SQL`, approved consistently for the group |
| `extraction_method` | `CETAS` or `COPY_ACTIVITY`; no implicit fallback |
| `mandatory_flag` | Whether failure blocks publication. Must be `true` for every enabled `RELEASE_POINTER` group member; an optional independent mapping may be `false` only when its own consumers tolerate stale data |
| `enabled_flag` | Controlled inclusion or exclusion |
| `expected_source_schema_version` | Expected authoritative hash/version of the ordered Synapse presentation-view result schema |
| `projection_contract_version` | Version of the structured ordered column contract used to generate CETAS and Databricks schemas |
| `delta_property_contract_version` | Immutable version of the governed Delta table-property contract applied to candidates, consumer tables or release-pointer physical tables as applicable |
| `size_category` | Small, medium, large or very large |
| `validation_tier` | Basic, standard or critical |
| `business_key` | Optional key used for reconciliation |
| `key_normalization_rule` | Source-compatible text comparison rule |
| `cetas_compatible` | Result of compatibility assessment |
| `compatibility_reason` | Reason for incompatibility or required casting |
| `copy_activity_compatible` | Approved compatibility result for the `COPY_ACTIVITY` route when selected |
| `copy_compatibility_reason` | Copy-route limitation, required setting or approval evidence |
| `compatibility_assessment_id` | Immutable reference to the latest approved method-specific compatibility assessment used by the current mapping roll-up |
| `allow_empty` | Whether zero rows are valid |
| `expected_min_rows` | Minimum expected rows when appropriate |
| `datetime_policy` | Mapping for wall-clock and instant timestamps |
| `datetimeoffset_policy` | UTC-normalisation and offset-retention decision |
| `float_tolerance` | Permitted tolerance for approximate numeric checks |
| `rollback_class` | Approved rollback/retention class, independent of validation tier |
| `staging_retention_class` | Approved retention rule for successful and failed Parquet attempts |
| `chunking_strategy` | `NONE`, `RANGE` or another approved deterministic strategy for very-large objects |
| `chunk_key` | Immutable extraction key used only when chunking is enabled |
| `column_mapping_mode` | Target Delta decision: normally `none`, `name` or `id` according to the approved compatibility baseline |

Maintain a child column-contract table owned by immutable `mapping_object_id` and keyed by that mapping plus contract version and ordinal. The target triple below is descriptive, denormalised reference data and is not the contract's authoritative key:

| Column | Purpose |
|---|---|
| `target_catalog`, `target_schema`, `target_table` | Descriptive target reference copied from the owning mapping; it must agree with that mapping |
| `projection_contract_version` | Projection-contract version joined to the parent mapping row |
| `ordinal` | Required output-column order |
| `source_column` | Source column when the projection is a direct reference |
| `source_expression_template` | Approved expression template for casts or conversions; not unrestricted user-entered SQL |
| `target_column` | Output column name |
| `target_type` | Databricks contract type |
| `precision`, `scale` | Decimal contract where applicable |
| `nullable` | Nullability expectation |
| `policy_classification` | Whether the target column requires a mask, tag or other governed policy |
| `cetas_risk_class` | Per-column CETAS risk such as standard, large-text or unsafe-character-sensitive |

Maintain a separate versioned Delta-property contract keyed by `mapping_object_id`, `delta_property_contract_version`, asset role and property name. Asset roles must distinguish at least `INDEPENDENT_CANDIDATE`, `INDEPENDENT_CONSUMER` and `RELEASE_POINTER_PHYSICAL`. The contract records the approved value, source of the requirement, minimum runtime, owner and whether the property must be rendered during creation/replacement or verified as a platform-managed feature. It must include, where applicable, `delta.deletedFileRetentionDuration`, `delta.logRetentionDuration`, `delta.setTransactionRetentionDuration`, `delta.columnMapping.mode` and every other governed Delta feature or operational property on which correctness, compatibility, rollback or performance depends. Do not copy arbitrary properties from the candidate to the consumer, and do not treat internal/platform-generated properties as user-managed configuration.

Maintain immutable inventory-version headers and member rows separately from the current mapping record. The current mapping is a governed authoring roll-up; it is not the execution-time source after a run is bound. Each inventory member snapshots whether the mapping is enabled and every run-affecting value selected for that wave, including group assignment, extraction method and approved compatibility assessment, publication and security modes, mandatory status, source-schema expectation, projection and Delta-property contract versions, validation settings, temporal/numeric rules, retention classes, chunking settings, size category and column-mapping mode. A bridge run references one approved inventory version, and orchestration reads these values from that immutable member snapshot. Any later change to enablement or another run-affecting value creates a new inventory version rather than changing the scope or behaviour of an active or historical run.

Maintain append-only compatibility-assessment history rather than overwriting the evidence behind the mapping's current `cetas_compatible` roll-up:

| Assessment level | Required recorded fields |
|---|---|
| Object | Source and target object, `assessed_at`, assessment method, `source_schema_version`, `projection_contract_version`, overall result and reason |
| Column profile | Source and target column, both version identifiers, risk class, `profiled_at`, maximum observed byte length, unsafe-character findings and profile result |

The mapping row's `cetas_compatible`/`compatibility_reason` and, when assessed, `copy_activity_compatible`/`copy_compatibility_reason` represent the latest approved method-specific object results. Compatibility history must record the extraction method, connector/runtime version and settings so prior findings show which assessment supports each roll-up.

Generate the ordered extraction projection and the explicit Databricks read schema from the same approved contract version. Restrict contract maintenance to the bridge deployment identity, validate expression templates against an allow-list and keep rendered SQL in the audit record. Quote identifiers and reject duplicate targets, duplicate ordinals, duplicate target columns and gaps in ordinals.

Target-column uniqueness must be checked using Databricks' case-insensitive identifier semantics, not only byte-for-byte comparison. Reject source results containing names that differ only by case unless the contract deliberately renames them to distinct target names. Quote legal identifiers in the source and target dialects, but do not assume quoting makes case-only duplicates valid.

The contract is ordinal-ordered for deterministic generation and comparison, while Parquet-to-Spark binding is validated by contracted field name. For every extraction, require the exact expected name at every ordinal, reject unexpected or missing fields, and project the DataFrame explicitly into the target order before writing. Do not silently rely on positional fallback.

Choose `delta.columnMapping.mode` before first table creation. Use `none` only when every target name satisfies standard Delta/Parquet restrictions and metadata-only rename/drop is not required. Use the organisation's tested `name` or `id` baseline when source-compatible names contain spaces or supported special characters, or when the associated protocol and downstream compatibility implications are accepted. Changing the mode later is a governed table migration, not an automatic loader action.

Calculate `expected_source_schema_version` reproducibly from authoritative ordered presentation-view result metadata: column name, ordinal, source type, length, precision, scale and nullability. Store the calculation algorithm/version with the hash. Do not rely on a manually incremented integer that can remain unchanged after drift.

The upstream run audit must also contain a monotonically increasing `upstream_sequence` or another authoritative source-snapshot order key. Business dates and string run IDs are descriptive and must not be used as the sole ordering rule.

Do not hard-code per-object CETAS statements in an orchestration pipeline, and do not use a single unrestricted free-text field as the authoritative projection contract.

### Supported Databricks execution baseline

Record and test the minimum Databricks Runtime, access mode and SQL warehouse channel used by every bridge read, write, policy-inspection and publication path. The baseline must support every Unity Catalog policy-inspection and enforcement feature and every exact write operation selected by the design. For ABAC-protected tables, use a supported SQL warehouse, serverless compute or Databricks Runtime 16.4 or later. For manually applied table-level row filters or column masks, validate the separate compute requirements for reads and writes; writes require Databricks Runtime 16.3 or later and a documented supported write pattern where runtime compute is used. Do not infer that support for reading a protected table proves support for the publication write or DDL. Revalidate the exact commands on the chosen runtime/warehouse channel before production and whenever that baseline changes.

## 3. Extraction routing, compatibility and temporal contracts

Before building or scheduling the bridge, assess every presentation view in the approved inventory.

### Declared data types

Inspect the result columns through Synapse metadata and verify that every exported column uses a CETAS-supported type. For an unsupported source type, explicitly cast it in the CETAS projection or assign the table to a different extraction method.

`uniqueidentifier` is supported in dedicated SQL pool tables but is not in the documented CETAS result-type list. Export every GUID through an explicit canonical cast, normally `char(36)` or `varchar(36)`, and record the target representation in the schema contract. Do not depend on an implicit conversion.

### LOB and text-content risks

Metadata alone cannot prove compatibility. For `varchar(max)`, `nvarchar(max)` and other large variable-length columns:

- Profile the maximum actual byte length.
- Confirm that no value exceeds CETAS's 1 MB LOB restriction.
- Test columns containing quotation marks, pipes, carriage returns or newlines because Microsoft documents possible CETAS-to-Parquet export errors for these characters.
- Run representative full-volume CETAS tests for risky tables.

Set `cetas_compatible = false` when a view cannot be exported safely without changing its projection. Do not discover these incompatibilities for the first time during production migration.

Compatibility is not permanently established by the initial assessment. Declared types may remain unchanged while later data introduces an over-1-MB value or unsafe text content. Record the object-level and per-column evidence in the compatibility-assessment history defined in §2. Reassess:

- whenever the source schema or projection contract changes;
- after a CETAS failure associated with a value or encoding condition;
- on an approved periodic cycle for columns classified as high risk; and
- before publication when source data-quality controls report a new maximum length or newly observed unsafe character pattern.

Periodic profiling provides early warning but does not replace runtime failure handling. Every nightly CETAS result must still be validated, and a failed or unreadable result must never be published.

### Alternate extraction route for CETAS-incompatible objects

Every enabled mapping must have an approved `extraction_method`; incompatibility must never cause silent omission or failure of unrelated compatible objects.

- `CETAS`: follow §§8–11 using the pre-created external data source and file format.
- `COPY_ACTIVITY`: use an ADF or Synapse pipeline Copy Activity to execute the same explicit ordered projection and write Parquet to the same immutable attempt-path convention.

For `COPY_ACTIVITY`, the orchestrator must use the same upstream run, `source_generation_id`, bridge run, release, mapping ID and attempt identifiers as CETAS. It must record the pipeline run/activity run IDs, source query/projection hash, rows read, rows written, files written, bytes written and final status. The sink must use a new empty attempt path and must not merge files from retries.

Copy Activity does not bypass the source-generation fence, schema contract, Parquet footer/readability validation, independent Parquet count, Delta decode-and-write, Delta reconciliation, policy gate or publication rules. Source and sink identities, integration runtime/network path, staging file format settings, fault-tolerance behaviour and rejected-row handling must be approved and tested. Skipping incompatible rows or silently truncating values is prohibited.

The group may continue extracting other objects after one method-specific failure, but its pointer cannot publish until every enabled member is `READY`. Objects not approved for either method must be explicitly disabled with an owner, reason, consumer-serving decision and remediation date; they remain outside the published bridge scope rather than disappearing from it.

### Temporal contract

Apply an explicit mapping:

| Synapse meaning/type | Databricks target |
|---|---|
| `date` | `DATE` |
| `time(p)` | Native `TIME(p)` only when the approved baseline is Databricks Runtime 18 LTS or later, the Beta feature is enabled and approved, and end-to-end Parquet compatibility has passed; otherwise use a fixed-width canonical string or integer microseconds since midnight under an explicit contract |
| `datetime`, `smalldatetime`, `datetime2` used as local/wall-clock values | `TIMESTAMP_NTZ` |
| A value that represents a real instant | Convert explicitly to UTC and store as `TIMESTAMP` |
| `datetimeoffset` | Normalise to a UTC `TIMESTAMP`; optionally retain the original offset in a separate column |

Do not rely on the Databricks session timezone to interpret Synapse `datetime2`. Validate timestamp values around Australian daylight-saving transitions where relevant.

For `time(p)`, preserve only time-of-day semantics—never attach a date or timezone implicitly. Record the selected representation, precision and rounding/truncation rule in the column contract. Native Databricks `TIME` is version- and preview-sensitive; if it is selected, include `00:00:00`, `23:59:59.999999`, source precisions `0` through `7`, and the approved loss-of-precision behaviour in integration tests. If the fallback is a string, require the invariant 24-hour representation `HH:mm:ss.ffffff` after the approved precision conversion. If the fallback is integer microseconds, require the inclusive range `0` through `86,399,999,999` and provide controlled consumer conversion views where needed.

Pin the Parquet calendar-rebase read contract for the selected Spark/Databricks baseline. Inspect representative CETAS and Copy Activity Parquet footers to determine whether timestamps use INT96, `TIMESTAMP_MICROS` or `TIMESTAMP_MILLIS`, then explicitly set and test the applicable `spark.sql.parquet.datetimeRebaseModeInRead` and `spark.sql.parquet.int96RebaseModeInRead` behaviour. Do not choose `LEGACY` or `CORRECTED` by assumption.

Include `0001-01-01`, the Gregorian transition boundary, the SQL `datetime` minimum and actual business sentinel dates in integration tests. Spark's normal `EXCEPTION` behaviour should fail ambiguous ancient values rather than permit an unnoticed shift; any approved `CORRECTED` or `LEGACY` setting must be justified by the observed writer semantics and locked in the job configuration. `1753-01-01` is a useful SQL boundary test but is not itself before the 1582 Gregorian transition.

## 4. Identity and permission model

Separate the following identities and responsibilities.

### CETAS storage identity

The identity contained in the Synapse database-scoped credential performs the ADLS access. If the external data source uses `IDENTITY = 'Managed Identity'`, this is normally the Synapse workspace managed identity.

Grant that credential identity the minimum required ADLS permissions on the Parquet staging scope, normally including the ability to create, write and list the CETAS output paths. Do not grant storage write permission to the pipeline identity merely because it submits the SQL, unless that identity also performs a separate storage operation.

### CETAS SQL execution identity

Use a non-human Microsoft Entra service principal created as a server login and mapped database user for CETAS execution. If that principal type is not supported by the organisation's tested dedicated-pool deployment, use a dedicated SQL-authenticated login whose secret is held in the approved secret store; do not fall back to a contained-only user because CETAS also requires login-scoped grants.

Grant the mapped **database user**:

- `SELECT` on the presentation views.
- `CREATE TABLE` in the dedicated-pool database.
- `ALTER` on the dedicated CETAS metadata schema (for example, `GRANT ALTER ON SCHEMA::<schema> ...`) or the documented `db_ddladmin` alternative; prefer the narrower schema grant.
- Permission to drop its temporary CETAS external-table metadata after successful or failed processing.

Grant the corresponding **server login**:

- `ADMINISTER BULK OPERATIONS`.
- `ALTER ANY EXTERNAL DATA SOURCE`.
- `ALTER ANY EXTERNAL FILE FORMAT`.

The ADLS writer remains the identity inside the database-scoped credential from the preceding subsection; do not grant storage access to the SQL login merely because it submits CETAS. Before holding the production source fence, run a deployment preflight as the exact login/user pair that creates and drops temporary metadata and writes a disposable test path.

Pre-create and protect the database-scoped credential, Hadoop external data source and external file format through an administrator-controlled deployment. Avoid broad database ownership.

The required `ALTER ANY EXTERNAL DATA SOURCE` permission is security-sensitive: Microsoft documents that it allows the principal to create or modify any external data source and thereby access database-scoped credentials in that database. Pre-creating the bridge objects does not remove this exposure. Therefore:

- Treat the CETAS execution principal as a highly trusted principal.
- Keep unrelated or higher-privilege database-scoped credentials out of the dedicated-pool database where operationally possible.
- Audit changes to external data sources, external file formats and credentials.
- Use ADF/Synapse Copy Activity instead of CETAS when the organisation cannot accept this CETAS privilege model.

Do not claim that a separate ordinary database automatically isolates CETAS. The presentation views reside in the dedicated-pool database; genuine isolation requires a separately validated architecture that can still access those source objects.

### Bridge orchestration/control identity

Use a dedicated non-human orchestration identity for the Azure SQL control database. Grant only execute rights on approved stored procedures, or narrowly scoped `SELECT`, `INSERT` and `UPDATE` rights on bridge-control tables when procedures are not used. The runtime identity must not own the database or alter control-table DDL. A separate deployment identity owns schema migrations and constraints.

Lease acquisition, renewal, takeover and state changes must run inside short Azure SQL transactions using enforced unique keys, expected state/control version and database UTC. The identity may read the closed Synapse upstream audit and submit approved Synapse/Databricks work, but it must not inherit the storage credential identities merely for convenience.

### Databricks job identity

Use a dedicated Databricks service principal as the production job's `Run as` identity. Keep it distinct from the deployment identity and, by default, from the storage-credential identity.

### Databricks staging-storage identity

Use the managed identity contained in an Azure Databricks Access Connector as the Unity Catalog storage-credential identity for the ADLS staging external location. Grant Azure storage RBAC/ACL permissions to this managed identity, not automatically to the job's service principal. Unity Catalog uses the storage credential to obtain path access on behalf of an authorised Databricks principal.

An Azure managed identity can alternatively be registered in the Databricks account/workspace and treated as a service principal, but that is a deliberate combined-identity design. If chosen, document both roles and prove that its effective Azure, workspace and Unity Catalog privileges remain least-privileged; do not describe the Access Connector identity and job `Run as` identity as interchangeable by default.

For the Parquet staging location, the job identity needs:

- Unity Catalog access through a storage credential and external location.
- `READ FILES` on the staging external location when reading the path directly.

The Access Connector managed identity behind that storage credential needs read/list access to the ADLS staging prefix. It does not need write or delete access merely because the job reads Parquet.

For Unity Catalog managed target tables the loading job identity needs:

- `USE CATALOG`.
- `USE SCHEMA`.
- `CREATE TABLE` on controlled candidate/physical schemas and `MODIFY` on physical snapshot tables to which it writes. The loading identity does not replace existing Strategy A consumer tables.
- `SELECT` on candidate, physical and consumer objects required for reconciliation, policy-metadata visibility and post-publication checks.
- Read-only `READ METADATA` on governed consumer securables where `SHOW POLICIES`, `SHOW EFFECTIVE POLICIES` or `DESCRIBE POLICY` is used.
- `SELECT` and narrowly scoped `MODIFY` on the Unity Catalog publication-control/history tables required by its assigned strategy. Azure SQL bridge-run, lease and approval mutations remain behind the orchestration/control identity; the Databricks task returns signed/identified outcomes to that orchestrator unless a separately approved combined identity is documented.
- Permission to run the assigned Databricks job compute or SQL warehouse.

The job identity may inspect table-level masks through `INFORMATION_SCHEMA.COLUMN_MASKS`, row-filter metadata, effective governed policies and consumer definitions, but it must not own consumer tables and must not receive `MANAGE` merely to make the policy gate work. `BROWSE` alone is not sufficient for complete column-mask inventory. Assign stable consumer-table ownership to an approved governance group.

### Strategy A publication identity

Run `CREATE OR REPLACE TABLE` through a separate, tightly scoped publication identity, distinct from the Parquet/Delta loading job. For each existing independent consumer table it replaces, grant `MANAGE` on that table, `CREATE TABLE` on its containing schema, `USE CATALOG` and `USE SCHEMA`, and `SELECT` on its validated candidate. Provision these grants before the first replacement and whenever a new consumer table is onboarded. Test the exact `CREATE OR REPLACE TABLE ... TBLPROPERTIES (...) AS SELECT ...` statement as this identity. The initial creation of a consumer table also requires `CREATE TABLE` on the schema; record and test the subsequent ownership and `MANAGE` assignment explicitly.

`MANAGE` can also change the table's security configuration. Treat this identity as privileged: scope its table grants to Strategy A targets, execute only version-controlled publication statements, restrict interactive use, and audit all its DDL and security changes. The governance group remains the controlled authority for deliberate policy remapping; a policy change by the publisher outside that workflow is an incident. If this privilege separation cannot be implemented, Strategy A cannot run under the documented replacement operation; resolve the publication method before enabling that target. The loading identity continues to perform read-only policy inspection and must not inherit the publisher's `MANAGE` grants.

Use UC managed Delta tables by default. If external Delta tables are mandatory, also grant the required external-location privileges, including `CREATE EXTERNAL TABLE`, and the corresponding ADLS write access.

### Staging-cleanup identity

Use a separate, tightly scoped cleanup service principal or controlled cleanup job identity. It needs access to the authoritative bridge/object-run audit and delete permission only on the Parquet staging prefix. When cleanup is executed through a Unity Catalog external location, grant `WRITE FILES` on that staging external location and ensure the backing storage-credential identity has the necessary ADLS delete permission. Do not grant deletion over Unity Catalog managed-table storage or unrelated container prefixes.

The cleanup identity must use audit-approved exact attempt paths. It must not discover deletion targets from unrestricted recursive listing, folder age alone, unresolved variables or user-supplied paths.

Delta candidate/release cleanup is a separate logical function. It may run as the bridge service principal because that principal owns the candidate tables it creates and already has `MODIFY` on approved physical snapshot tables, or as a dedicated Delta-maintenance service principal. A separate identity needs `USE CATALOG`, `USE SCHEMA`, `SELECT` plus `MODIFY` on the physical tables it cleans, and ownership or `MANAGE` on each candidate table it is authorised to drop. It must not receive `MANAGE` on consumer tables.

### Databricks policy-governance identity

Use a separate governance group or controlled deployment identity for policy changes. When an existing table-level row filter or column mask must be added, removed or remapped, that identity—not the bridge job—performs the approved change. For a manually assigned table-level filter or mask, use table ownership or `MANAGE` plus `SELECT`, together with `USE CATALOG`, `USE SCHEMA` and `EXECUTE` on the policy function where applicable. For an ABAC policy change, require ownership or `MANAGE` on the catalog, schema or table where that policy is defined.

The bridge must stop the affected target at the policy gate and create an approval item containing the schema diff, current policy binding and proposed binding. Publication resumes only after the governance change is approved, applied and independently re-inspected. The bridge must never silently drop, move or weaken a policy.

The governance group must also define the controlled rollback identity and policy prerequisites. Where ABAC applies, add that identity to the relevant policy `EXCEPT` scope only when necessary for approved time-travel, backup or recovery operations; keep the exception narrow and audited.

### Restricted validation identities

Maintain non-human test service principals or groups representing the important consumer access profiles. Post-publication policy tests must execute as these restricted identities. A query executed only as the privileged bridge, owner or governance identity does not prove what a business user can see.

### Business users

Business users should receive only:

- `USE CATALOG`.
- `USE SCHEMA`.
- `SELECT` on stable consumer tables or views.

They should not have direct access to Parquet staging, candidate tables, unpublished releases or writable publication-control objects.

## 5. ADLS staging structure

Use immutable attempt-specific folders. Avoid Hive-style `key=value` folder names unless partition discovery is intentionally required.

```text
/bridge/parquet-stage/<business-date>/<bridge_run_id>/<mapping_object_id>/attempt-<number>/
```

Every CETAS attempt must use a new empty directory:

```text
.../<mapping_object_id>/attempt-<new-number>/
```

Use the immutable `mapping_object_id`, or a collision-resistant encoded schema/object identifier, in the path. Do not use an unqualified table name: two schemas can contain the same name. Store the canonical leaf path in the object-run audit and validate that it remains underneath the configured staging root.

Never retry into a failed attempt's directory. Read each table's exact leaf path from Databricks instead of recursively listing the complete bridge-run folder.

Parquet staging is temporary transport storage. It is not the published Delta layer.

### Sensitive-data controls for staging and candidates

Parquet staging, independent candidate tables and unpublished release slices contain unmasked source values. Treat them as governed copies of the highest data classification present in their source object; consumer-layer masks and row filters do not protect these locations.

- Register data owner, classification and sensitivity tags in the control metadata and Unity Catalog where applicable.
- Use encryption at rest and TLS in transit. Record whether platform-managed keys satisfy policy or whether customer-managed keys are required for ADLS, Azure SQL and Databricks managed storage; CMK is an explicit governance decision, not an assumed universal requirement.
- Deny business-user access and use separate least-privileged staging, loading, validation and cleanup identities as defined in §4.
- Enable Azure Storage, Azure SQL, Synapse and Databricks audit/diagnostic logging required by the incident and compliance policy.
- Apply the same legal hold, breach-response, residency and approved retention classifications used for the corresponding consumer data.
- Prevent staging paths and candidate schemas from being exposed through broad external locations, mounts, shares or catalog grants.
- Test access negatively as an ordinary business identity and positively as each named operational identity before production.

## 6. Complete the existing Synapse materialisation

The existing Synapse transformation and materialisation logic remains unchanged. Its orchestration entry and exit points participate in the source-fence interlock:

1. Run existing Synapse ingestion.
2. Execute the existing transformation logic.
3. Write each transformed-view output to its corresponding materialised Synapse table.
4. Keep the simple presentation view over that table.
5. Record completion and row count for each materialised table.
6. Close the upstream run only after every enabled table required for its publication target or group has finished and acquire the source-generation fence described below. The fence prevents every registered upstream writer from beginning another mutable generation while CETAS or Copy Activity is reading the closed generation.

The fence is mandatory because dedicated SQL pool uses `READ UNCOMMITTED` by default unless the database has been deliberately changed to `READ_COMMITTED_SNAPSHOT`. Under the default, an extraction overlapping refresh can observe uncommitted changes. Even with `READ_COMMITTED_SNAPSHOT`, each extraction statement receives its own statement-level view rather than one cross-statement transaction, so the write fence or immutable generation-specific source tables are still required for group consistency.

The upstream audit must contain one row per materialised table with `upstream_run_id`, `business_date`, `materialised_table`, `status`, `row_count`, `refresh_start`, `refresh_complete`, `source_schema_version`, monotonic `upstream_sequence` and `source_generation_id`.

The run-level record should confirm:

```text
expected_enabled_mappings   = <inventory-derived count>
successful_enabled_mappings = <same inventory-derived count>
failed_mandatory_mappings   = 0
status                      = COMPLETE
write_closed                = true
source_fence_state          = BRIDGE_HELD
```

### Enforceable source-fence mechanism

The Data Operations Owner owns the interlock; the Synapse Platform Owner owns the removal of bypass write paths. Implement one authoritative `source_generation_fence` row in the Azure SQL control database for the complete mutable source-table set, or one row per independently writable source set. Enforce its allowed states and transitions through stored procedures and short transactions:

```text
AVAILABLE -> WRITE_ACTIVE -> CLOSED -> BRIDGE_HELD -> AVAILABLE
```

Exceptional recovery uses an audited `ABANDONED` state: `WRITE_ACTIVE`, `CLOSED` or `BRIDGE_HELD` may move to `ABANDONED` only through the takeover/incident procedure, after which a conditional transition allocates a new generation and returns the source set to `AVAILABLE`. Normal scheduling must never use `ABANDONED` as a shortcut around an active owner.

The row must contain at least `source_set_id`, state, `source_generation_id`, upstream writer owner/run, bridge owner/run, acquisition time, expiry time, heartbeat time and `control_version`. A conditional transition succeeds only from the expected state and version; exactly one affected row is required. Lease expiry does not by itself authorise a conflicting writer: the takeover procedure must first prove the previous Synapse work is terminal, record the abandoned owner and generation, and allocate a new generation where required.

Every scheduled, event-triggered, support and replay process capable of modifying a governed materialised table must call the same procedure and enter `WRITE_ACTIVE` before its first write. It must retain and heartbeat that ownership until the generation is complete or abandoned. The bridge may transition `CLOSED` to `BRIDGE_HELD` only after independently confirming that all required upstream completion rows, including every enabled release-pointer group member, belong to the same generation and that no source writer is active. While `BRIDGE_HELD`, the procedure rejects every attempt to enter `WRITE_ACTIVE`. Releasing the bridge fence conditionally returns the row to `AVAILABLE` only for the expected generation, bridge owner and control version.

Restrict normal Synapse write permissions to registered upstream runtime identities whose deployment and entry points enforce this protocol. Remove direct write permission from obsolete schedulers and ordinary support identities. Emergency administrator bypass cannot be technically prevented from every privileged principal; treat it as a break-glass action that must cancel or abandon the active bridge generation and raise an immediate alert. Therefore this is an enforced cooperative orchestration fence, not a dedicated-pool lock held across all extraction statements.

### Fence-violation detection

Detection is a layered backstop; it does not convert the cooperative fence into an immutable snapshot. Implement all applicable layers:

1. **Control-state detector.** Stream or poll the authoritative fence-transition audit. Alert on an unexpected owner, generation or state/version transition; rejected `WRITE_ACTIVE` acquisition; direct table update outside the approved stored procedures; expired/missing heartbeat; or a fence row that changes while the bridge owns `BRIDGE_HELD`. A changed generation or control version makes the current extraction ineligible for publication until reconciled.
2. **Synapse statement-audit detector.** Enable Synapse SQL auditing to an approved Log Analytics, Event Hubs or storage destination and monitor for `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, CTAS/table-swap and relevant DDL statements against governed materialised tables whose event time overlaps `BRIDGE_HELD`. Maintain the governed object/principal inventory used by the detection query. Any matching write—including one by a privileged or normally approved runtime identity—is a fence violation unless it is the separately documented immutable-generation operation outside the fenced source objects.
3. **Audit-health detector.** Alert on disabled/deleted audit or diagnostic settings, destination/permission failures, ingestion delay beyond the approved threshold and absence of expected audit-health signals. The bridge must not claim bypass monitoring is healthy when the audit path is unavailable.
4. **Source-state smoke checks.** Before releasing `BRIDGE_HELD`, re-read the authoritative upstream completion rows, fence generation/version and current presentation schema metadata. Recheck an exact source row count for critical mappings and for every mapping whose audit/control signals are incomplete or suspicious. Use an approved stronger data fingerprint for critical mappings when its cost and source semantics have been proven. A row-count or schema mismatch is evidence of mutation; equality is not proof that no values changed.

Azure SQL/Synapse auditing is optimised for availability and can omit events under high load, and statement delivery may be delayed. Row count and `source_schema_version` also miss equal-count, schema-preserving data changes. Record those residual risks explicitly. If absence of modification must be guaranteed independently of writer compliance and best-effort detection, use immutable generation-specific source tables.

Persist every positive or indeterminate detector outcome with `source_set_id`, generation, bridge run, event source/time, principal, statement/request reference where available, affected objects/groups, classification and disposition. A positive fence violation, unexplained audit gap or mismatching smoke check stops publication of the affected consistency group, closes affected source-read attempts as `ABANDONED_GENERATION` and requires a new generation/release. Independent mappings may be isolated only when the evidence proves their source set was unaffected.

`write_closed` is evidence from the upstream run, not the fence itself. Publication evidence must retain the conditional fence-transition audit, the inventory of registered writers and a concurrency test showing that a refresh cannot enter `WRITE_ACTIVE` while the bridge owns `BRIDGE_HELD`. Hold `BRIDGE_HELD` until every CETAS or Copy Activity source read for the bridge run has reached a terminal state and no retry will read the original source generation. Delta loading, reconciliation and transient Delta retries normally reuse the retained immutable Parquet attempt and therefore do not require the source fence to remain held. Release it explicitly once no further source read is possible.

Define and approve `maximum_source_fence_duration` from the upstream schedule and measured extraction critical path. Heartbeat and monitor the fence independently from the bridge lease. When the fence approaches expiry, stop submitting new source reads and allow already-running requests to reach a known terminal/cancelled state. If the fence expires or must be released before a required source re-extraction, abandon the incomplete source-read portion of that release: do not combine a newly mutable generation with completed old-generation extracts. Close affected attempts as `ABANDONED_GENERATION`, acquire a new closed `source_generation_id`, create a new release identity and re-extract every enabled member of the affected consistency group. Independent targets may restart individually when their contracts allow independent publication.

If the organisation cannot inventory and bind every source writer to this interlock, or cannot restrict bypass write permissions, materialise each upstream run into immutable, generation-specific source tables instead. No extraction method may overlap a refresh that can change its source objects.

## 7. Start and identify the bridge run

Start the bridge through one of these mechanisms:

1. The upstream process invokes the bridge after successful completion.
2. The upstream process writes an authoritative completion event/marker that triggers the bridge.
3. A polling guard detects a completed and closed upstream run.

Even when triggered by an event, the bridge must independently verify the upstream audit state.

### Prechecks

Before extraction:

- Verify the expected business date and upstream run ID.
- Verify upstream status is `COMPLETE` and `write_closed = true`.
- Verify all enabled release-pointer group members and every independent target selected for this run have completed upstream materialisation.
- Verify the mapping contains no duplicate source or target objects.
- Bind the run to one immutable `inventory_version`. Verify every enabled mapping belongs to one approved consistency-group contract version; reject `UNASSIGNED`, mixed-version groups, partially enabled groups and unapproved `INDEPENDENT` classifications.
- Verify each enabled group has current blast-radius measurements and required availability acceptance, including Data Architecture Owner approval when an escalation threshold is exceeded.
- Reject every enabled `RELEASE_POINTER` mapping with `mandatory_flag = false`; a group cannot publish with an absent member slice.
- Verify every enabled mapping has one approved route: `CETAS` with `cetas_compatible = true`, or `COPY_ACTIVITY` with an approved copy compatibility assessment. Reject only the invalid mappings and block their affected publication groups; do not silently omit them or fail unrelated independent targets.
- Verify every enabled mapping resolves one applicable `delta_property_contract_version`, that all correctness-sensitive properties have approved values, and that the selected Databricks runtime/warehouse supports the required table features.
- Verify no bridge run for the same upstream run is active or already published.
- Acquire a renewable bridge execution lease, or per-target/consistency-group leases, that prevents two runs from loading or publishing the same destination concurrently.
- Verify that the candidate `upstream_sequence` is greater than the durable publication high-water sequence for every affected target/group. The high-water value records the greatest source sequence ever successfully published and never decreases during rollback. Reject stale and equal sequences unless an explicitly authorised correction/replay is tied to a recorded `BAD` release and uses a new release identity.
- Verify the DW2000c pool is online.
- Verify the staging location is accessible.
- Verify each `expected_source_schema_version` and `projection_contract_version` independently.
- Verify the authoritative fence row is `BRIDGE_HELD` by this bridge run for the expected `source_generation_id`, its heartbeat is healthy and no registered writer is `WRITE_ACTIVE`.
- Verify the fence-transition, Synapse statement-audit and audit-health detectors are enabled, within their approved ingestion-latency thresholds and bound to the current source-object/principal inventory. Treat an unexplained monitoring gap according to §6 rather than assuming no write occurred.
- Verify the approved source-fence budget, extraction/load/reconciliation critical-path budget and enough remaining fence time for the scheduled source reads. Refuse to start work that cannot reasonably finish before the fence deadline.

Create linked identifiers:

```text
upstream_run_id = <upstream_run_id>
bridge_run_id   = <bridge_run_id>
release_id      = <release_id>
```

Insert a bridge-run record with status `EXTRACTING`. The same IDs must connect extraction, Parquet validation, Delta loading, reconciliation, publication and rollback history.

Use controlled run states such as:

```text
CREATED -> EXTRACTING -> PARQUET_VALIDATED -> DELTA_LOADING
        -> READY -> PUBLISHING -> PUBLISHED
```

`FAILED`, `CANCELLED` and `BAD` are terminal or recovery states with recorded reasons. Object attempts have their own state and immutable attempt number. State changes must be conditional updates from the expected prior state so two orchestrators cannot both advance the same work.

The execution/publication lease must have an owner, expiry and heartbeat. A replacement process may take over an expired lease only after inspecting Synapse request status, exact ADLS attempt contents, Delta table history/transaction identifiers and publication-control state. Never treat a timeout or lost client acknowledgement as proof that the preceding operation failed.

## 8. Extract the simple presentation views

Route each enabled mapping by its approved `extraction_method`. Both methods must render the same versioned ordered projection and produce Parquet under the same attempt-path and audit contract.

### CETAS route

For every enabled mapping:

```text
Simple presentation view
    -> unique temporary external-table name
    -> unique attempt-specific ADLS folder
    -> Parquet files
```

Each operation must use:

- An explicit ordered column list rendered from the approved column-contract version, not `SELECT *`.
- A unique external-table name.
- A unique empty output directory.
- A query label containing the bridge run and source object.
- A recorded Synapse request ID.
- Per-object start, end and retry status.
- Explicit error capture.

After the CETAS request reaches a terminal state, execute a `finally`-style metadata cleanup that drops the temporary external-table definition on both success and failure when it exists. On success, wait until the files no longer need to be addressed through that external table. On failure, retain the attempt files according to the incident rule even though the metadata is dropped. Dropping external-table metadata does not remove ADLS files.

### Copy Activity route

For a mapping approved as `COPY_ACTIVITY`, execute the explicit projection through the approved source connector and integration runtime. Write Parquet only to the mapping's new empty attempt leaf path. Record the ADF/Synapse pipeline and activity run IDs and the metrics required in §3. Treat the activity as successful only when the service reports success, no rows were skipped or truncated, the exact sink path is closed and the common validation in §§10–13 passes.

Do not allow the Copy Activity to infer a different schema, normalize names differently, change timestamp semantics or apply automatic fault tolerance that weakens the common contract. Method-specific settings belong to versioned deployment configuration, not unrestricted mapping-table text.

### Optional chunked extraction for very-large objects

Chunking is **PHASE 2** unless testing proves that an object cannot satisfy the fence/retry objective as one extraction. It requires an immutable non-null key, approved non-overlapping predicates and a pre-submission manifest containing the mapping, generation, release, attempt, chunk bounds or predicate hash, status and exact leaf paths. Each chunk uses a separate path and request ID. Publish only after every leaf is independently validated, the manifest has no gap or overlap, all columns decode and the union count reconciles to the authoritative whole-object count. A changed plan, fence loss or generation requires a new object attempt; never mix attempts or generations.

## 9. Concurrency at DW2000c

DW2000c supports substantial concurrency, but the documented maximum is a ceiling, not the correct operating level for this workload.

Dynamic `smallrc` workloads can run up to 32 concurrent queries at DW2000c. Microsoft documents a service-level ceiling of up to 48 concurrent workload-group queries at DW2000c with the minimum supported 2% request grant. This is a platform ceiling, not `100 / 2 = 50`; use the published service-level limit rather than deriving a higher value arithmetically. A 2% grant can increase concurrency but may make individual exports slower. See [Microsoft's dedicated SQL pool concurrency table](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/memory-concurrency-limits#concurrency-maximums-for-workload-groups).

Use a dedicated workload group when isolation from other Synapse activity is required. Create a workload classifier that maps the CETAS execution principal, role, `WLM_LABEL` or controlled session context into that group. Configure `GROUP_MAX_REQUESTS` to the tested operational cap rather than leaving it unlimited, and establish the final resource grant and concurrency through full-volume testing.

During testing and production monitoring, inspect Synapse request/workload DMVs to prove that CETAS requests were assigned to the intended group and classifier. Treat a request that falls into `smallrc` or another unintended group as a configuration failure; do not interpret its performance as evidence for the dedicated-group settings.

Recommended test sequence:

```text
8 -> 12 -> 16 -> 24 -> optionally 32
```

Test 32 only when 24 still improves aggregate throughput. Do not target 48 merely because it is the documented maximum.

Measure:

- Total exported GB per minute.
- Per-table execution time.
- Queue duration.
- CPU and data-movement pressure.
- ADLS throttling and HTTP 503 responses.
- Impact on any other permitted Synapse workload.
- Long-tail completion time.

Before production, turn these measurements into an approved wall-clock budget even though this design does not prescribe the overnight window. Record p50/p95 duration and throughput targets by size/method for source extraction, Parquet validation/count, Delta decode/write, reconciliation and publication; calculate the critical path by consistency group. Include queueing, bounded retries and a safety margin before `maximum_source_fence_duration`. A run that breaches its phase or fence threshold must stop new work, alert operations and follow the fence-expiry rule in §6.

Schedule the largest tables early, mix medium tables behind them and use small tables to fill free capacity. Retry only failed objects.

Do not assume that CETAS will produce exactly 60 files per table. File count depends on execution parallelism and data volume. Measure actual results for representative small, medium and large tables.

### Copy Activity capacity and concurrency

Treat Copy Activity as a separately capacity-governed extraction route that shares the same Synapse source budget and fence deadline as CETAS. Before enabling a mapping for `COPY_ACTIVITY`, record in version-controlled deployment configuration:

- Azure integration runtime or self-hosted integration runtime, region and network path;
- connector and sink-format versions;
- configured or `Auto` `dataIntegrationUnits` when Azure integration runtime is used;
- `parallelCopies`, source partition option and deterministic partition bounds where parallel source reads are enabled;
- maximum concurrent Copy Activities by small, medium, large and very-large size class;
- source query timeout, retry ceiling, queue timeout and activity timeout; and
- the combined CETAS-plus-Copy ceiling enforced by the bridge scheduler.

DIUs do not apply to a self-hosted integration runtime. For self-hosted runtime, record node count, CPU, memory, network throughput, concurrent-job limit, high-availability arrangement, patch ownership and scale-up/scale-out thresholds. Do not assume that additional integration-runtime capacity removes the Synapse workload limit: parallel source partitions and concurrent activities still increase source queries and resource pressure.

Establish the Copy Activity operating point with full-volume tests, beginning conservatively. Use the following default ladder unless a lower environment-specific ceiling is approved:

```text
Concurrent Copy Activities: 2 -> 4 -> 8 -> 12 -> optionally 16
parallelCopies per activity: 1 -> 2 -> 4 -> optionally 8
```

Change only one dimension at a time. Test each relevant size class and representative CETAS/Copy mixtures. Increase a setting only while aggregate throughput improves without breaching the approved Synapse queue, CPU/data-movement, integration-runtime, ADLS-throttling or p95-duration threshold. Stop the ladder when throughput flattens, long-tail duration worsens, the source is saturated or remaining fence margin falls below the approved safety reserve. The tested result—not the ladder maximum—becomes the production cap.

The scheduler must account for each Copy Activity's expected number of source partitions and must not treat one pipeline activity as one unit of Synapse pressure. Reserve capacity for already-running requests, bounded retries and mandatory large objects. When CETAS and Copy Activity overlap, enforce a single aggregate source-read budget and record queue time, DIUs actually used, effective parallel copies, rows/bytes read and written, throughput, Synapse request IDs, integration-runtime saturation and ADLS throttling for every activity.

Calculate the critical path separately for CETAS-only groups, Copy-only groups and mixed-method groups, then include the slowest mandatory path, queueing and retries in `maximum_source_fence_duration`. A Copy route that cannot finish within that approved budget is not production-compatible until it is reconfigured, chunked under the governed chunking design, assigned an immutable generation-specific source, or removed from bridge scope through the explicit disabled-object process.

### Databricks load concurrency

Control Databricks concurrency independently from Synapse CETAS concurrency. Do not submit the complete Parquet-to-Delta inventory simultaneously.

Use a separate full-volume test sequence such as:

```text
4 -> 8 -> 12 -> 16 concurrent table loads
```

Set separate limits for small, medium, large and very-large tables. Determine the production limits from job-compute or SQL-warehouse capacity, worker memory, ADLS request throttling, Delta commit behaviour, file sizes, small-file generation and the effect on other workloads. Increasing Synapse extraction concurrency does not require the same Databricks load concurrency.

Measure Parquet counting and the decode-and-write phase separately. Record scan time, write time, bytes read/written and failure classification so the cost of full decoding is visible in capacity tests.

## 10. Validate the extraction output

After each CETAS operation or Copy Activity:

- Confirm the Synapse CETAS request or Copy Activity completed successfully and record its authoritative request/activity identifier.
- Confirm the attempt folder exists.
- Confirm readable Parquet output exists, unless an empty result is allowed.
- Enumerate only the files in that exact leaf path and confirm that every Parquet footer is readable.
- Apply the expected schema instead of relying on inference.
- Confirm ordered column names and compatible types.
- Confirm decimal precision and scale.
- Record file count and total bytes.
- Confirm failed and successful attempt files are not mixed.
- Enforce `allow_empty` and `expected_min_rows`.

A failed or cancelled CETAS request is different: Synapse makes only a one-time best-effort attempt to remove newly created files and folders, so its attempt path may be absent, partially removed or still present. Classify the failure from the recorded Synapse request status and error first. Folder state is diagnostic evidence only and must never make a failed request eligible for validation or loading. Before a retry, close the failed attempt, retain or clean its remnants under the audited retention rule, and use a new empty attempt path.

### Separate row counting from all-column decode evidence

Do not use `count()` as proof that every Parquet column can be decoded. Spark can satisfy a Parquet row count without reading the values in every projected column.

The process needs two distinct pieces of evidence, but it should not perform two full value scans by default:

1. **Exact row count:** calculate the independently executed row count over the exact attempt leaf path.
2. **All-column decode evidence:** obtain this from the candidate Delta write in §12, which reads every contracted Parquet column, applies required conversions and materialises the result. A successfully committed candidate write is the decode evidence; Delta row count is still queried separately afterward.

The decode-and-write action must:

- reference every expected top-level and required nested field;
- fail on unreadable files, incompatible physical/logical types and conversion errors;
- run with corrupt-file and missing-file skipping disabled;
- write every contracted target column rather than projecting problematic columns away; and
- have its physical plan inspected during implementation testing to confirm that the expected columns are present in the Parquet scan feeding the Delta write.

This deliberately avoids a separate pre-load all-column hash scan followed by another full scan for the Delta load. If an implementation chooses a separate decode pass, it must measure and approve the additional scan cost. `validation_tier` controls additional business reconciliation, not whether all contracted columns must be decodable.

Record:

```text
source view
materialised table
target table
upstream run ID
bridge run ID
release ID
CETAS request ID
attempt number
start and completion times
queue duration
execution duration
file count
Parquet bytes
status
error details
```

Do not treat successful CETAS statement or Copy Activity completion as the only validation.

## 11. Source-to-Parquet reconciliation

Avoid running another full set of `COUNT(*)` queries over Synapse merely for reconciliation when the trusted upstream materialisation audit already contains the source row counts.

Use:

```text
Audited materialised-table row count
    = independently executed readable Parquet row count
    = independently queried committed candidate Delta row count
```

This is the final three-way reconciliation objective. §11 establishes the source-to-Parquet side; the candidate is created in §12 and its independently queried count completes the equality in §13.

For Parquet, execute an independent count over the exact immutable attempt leaf path using the explicit expected schema. Do not claim that this count proves every column was decoded. The successfully committed decode-and-write action in §12 supplies that evidence. Do not derive the only Parquet count from the Delta write result or reuse the candidate Delta count as the Parquet count.

Parquet footer row counts may be summed as an additional structural check, but they are not a substitute for successful decode-and-write: footer metadata alone does not prove that all columns can be decoded using the expected schema or that required conversions succeed.

If a simple presentation view intentionally filters the materialised table, the upstream process must record the presentation result count or that object must use an approved view-level count. Do not compare a filtered view with the unfiltered table count.

For an object with `allow_empty = true`, zero rows are accepted only when the CETAS request completed successfully and the durable upstream audit (or approved presentation-view audit) independently records an expected zero. An empty or absent folder by itself never proves a legitimate empty result.

Synapse request DMVs are useful for operational telemetry and diagnosis, but should not automatically replace durable business reconciliation records.

### Mandatory checks for every table

- Exact row count.
- Exact ordered column names.
- Contract-compatible data types.
- Decimal precision and scale.
- Timestamp mapping.
- Release, bridge and upstream IDs.
- Empty-table policy.

### Tiered business checks

Run additional checks according to `validation_tier`:

- Critical-column null counts.
- Normalised business-key distinct counts.
- Normalised duplicate-key counts.
- Minimum and maximum business dates.
- Important financial and quantity totals.

For textual keys, reproduce the agreed business semantics on both sides. Synapse comparisons may be case-insensitive and may treat trailing spaces differently from Spark. Use an explicit rule such as `UPPER(TRIM(key))` only when that rule correctly represents the source business contract.

Use exact equality for `decimal`, `numeric`, `money` and integer totals. Use an approved absolute or relative tolerance for `float` and `real` totals.

Run expensive full business reconciliation for finance, regulatory and other critical tables. Use scheduled rotating samples for lower-risk tables where a full second validation pass is not operationally justified.

## 12. Load candidate data into Unity Catalog Delta

Parquet files are not Delta tables. Databricks must write the data into a Delta transaction log.

For each validated Parquet leaf path:

1. Read using the explicit expected schema.
2. Apply the approved type conversions, including timestamp policy.
3. Use the previously completed readable Parquet count as the source-to-Parquet reconciliation result; do not infer it solely from Delta write metrics.
4. Add immutable operational metadata for both publication strategies:
   - `release_id`
   - `business_date`
   - `upstream_run_id`
   - `upstream_sequence`
   - `source_generation_id`
   - `group_contract_version` for release-pointer members
   - `bridge_run_id`
   - `mapping_object_id`
   - `load_attempt`
   - `loaded_at`
5. Before writing a release-pointer candidate into a shared physical snapshot table, pass the Strategy B contract/policy gate in §13.
6. Write every contracted column to candidate Delta data. A successful atomic commit is the mandatory all-column decode evidence described in §10.
7. Classify failures before retry: Parquet decode, schema and deterministic conversion failures are non-retryable until the data or contract is corrected; transient ADLS, compute or Delta commit failures may follow the bounded retry policy.
8. Use optimized writes so the extraction file layout is not copied directly into Delta.
9. Capture Delta scan, conversion, write and commit metrics separately where available.

### Retry and ambiguous-commit rule

Every Delta write must be retry-idempotent. Derive one stable logical write key from the immutable `mapping_object_id` and `release_id`. The Delta-load attempt is submission/recovery metadata only: it never enters the logical write key or the transaction pair. Because each mapping has exactly one unique target, `mapping_object_id` is also the stable target writer identifier. Any correction or other intentionally different data requires a new `release_id` and a new logical write.

For DataFrame writes that support Delta transaction identifiers, use this required derivation rule:

- `txnAppId` is a deterministic identifier for the target writer and logical release, for example `bridge:<mapping_object_id>:<release_id>`. A correction that intentionally writes different data must use a new release ID and therefore a new `txnAppId`.
- `txnVersion` is the non-negative Azure-SQL-enforced `delta_write_sequence` allocated once for that logical write. It is stored before submission, never manually reset or reused for different data, and remains unchanged for every retry of that same write.
- Set non-sensitive Delta `userMetadata` containing the logical write key, mapping/release/attempt identifiers and transaction pair so `DESCRIBE HISTORY` provides authoritative operational correlation without parsing file names.
- A rebuilt control database, replay tool or recovered run must restore the recorded pair. If that cannot be proven, allocate a new release/logical write rather than guessing a version.

Delta uses the pair to ignore duplicate writes. Therefore a successful client return is not proof that new rows were committed: after every submission, query Delta history for the expected pair/commit metadata and independently reconcile the exact committed candidate or release slice count. A skipped duplicate is success only when the already-committed slice has the expected logical identity and reconciliation results.

For every table that relies on this transaction-pair duplicate suppression, set `delta.setTransactionRetentionDuration` at initial creation to at least the maximum approved retry and disaster-recovery horizon plus an explicit safety margin. Never allow it to be shorter than the longest period in which the same logical write may be resubmitted. A retry after that horizon must not reuse an expired transaction pair; stop and follow the approved recovery procedure.

If the selected batch-write API/runtime does not support these transaction identifiers, use an idempotent `MERGE`/replace design or write to a uniquely named candidate and promote it only after verifying its committed identity.

Do not automatically repeat a write when the client times out or loses the commit acknowledgement. First inspect Delta history and the target release slice using the logical write key. Continue from the committed result if it is complete; retry only when the authoritative evidence proves there was no successful commit. Record the Delta table version, operation ID and logical write key in the object-run audit.

### Independent tables

Load into a uniquely named controlled candidate table, validate it and publish with `CREATE OR REPLACE TABLE ... AS SELECT ...` or the equivalent controlled replacement operation. Candidate identity must include the immutable mapping ID, bridge run and load attempt, for example:

```text
candidate.<mapping_object_id>__<bridge_run_id>__a<load_attempt>
```

Quote/sanitise identifiers through the approved name generator and enforce platform length limits. Never reuse or overwrite a candidate belonging to another logical attempt. Store its fully qualified name and committed Delta version in the audit record.

### Release-pointer consistency groups

Write the candidate release into a physical snapshot table and keep the release metadata required by the consumer view:

```text
physical.customer_snapshots
physical.sales_snapshots
physical.product_snapshots
```

The physical table can temporarily contain current and previous releases. Stable consumer views expose only the release selected for their consistency group.

Before writing, prove that no successful slice already exists for the same `mapping_object_id` and `release_id`, except when authoritative Delta transaction history identifies the operation as the same idempotent retry. After commit, assert that the slice has one logical load identity and reconcile only that slice. A retry must never append a second copy of the same release.

For a multi-release physical table, test the release-pointer view on representative large tables and inspect the query profile and physical plan for files read, bytes read and pruning. The control-table scalar subquery can require dynamic file pruning, whose effectiveness depends on query shape, compute/runtime features, table size and file layout. If profiling shows excessive scanning, evaluate `release_id` as a liquid-clustering key or, for sufficiently large releases, as a partition column. Do not mandate either layout for every table; small tables and tables retaining very few files may not benefit.

Prefer UC managed physical tables. Do not partition every table automatically. Consider a release partition only for sufficiently large multi-release tables and confirm it does not create undersized partitions.

Use `OPTIMIZE ... WHERE release_id = ...` only when `release_id` is a partition column and the predicate is supported. On a liquid-clustered table, partial reclustering requires `OPTIMIZE <table> FULL WHERE <supported-predicate>` on Databricks Runtime 18.1 or later; only supported simple range predicates on one clustering column are allowed. Record that runtime requirement in the §2 execution baseline. For small managed tables, optimized writes and Databricks-managed file-size tuning are normally preferable to blanket manual `OPTIMIZE` commands.

## 13. Delta reconciliation and readiness

For every candidate:

```text
Validated Parquet rows = candidate Delta rows
```

Obtain `candidate Delta rows` through a separate query against the successfully committed candidate table. Do not compare two metrics produced by the same write operation.

For Strategy B, “candidate” means only the new release slice in the shared physical table. Every row count, null/key check and business reconciliation must filter by the audited `release_id`, `bridge_run_id` and `mapping_object_id` (or the equivalent immutable object key). Also assert that the slice contains exactly one approved load-attempt identity. Never count the whole multi-release physical table.

Validate:

- Exact row count.
- Exact expected schema.
- Release ID when applicable.
- Business date.
- Upstream and bridge run IDs.
- Validation-tier business checks.
- Delta file count and file sizes.
- A successful query through the intended SQL warehouse.
- The committed Delta version/operation identity expected for the logical write.
- Effective governed Delta properties and table features against the applicable candidate or release-pointer physical-table property contract.

### Downstream-consumer compatibility gate

Maintain a governed inventory of every consumer of the stable target: interactive SQL/Power BI, Structured Streaming, change data feed, Lakeflow Declarative Pipeline or materialized-view dependencies, Delta Sharing/OpenSharing recipients, extracts and external applications. Record owner, access mode, schema expectations, refresh/checkpoint behaviour, service-level objective and notification route.

Before first publication and every contract or publication-mechanism change:

- classify whether a full replacement is an append, overwrite/non-append change or schema change from that consumer's perspective;
- test Structured Streaming behaviour, checkpoint recovery and duplicate/delete semantics rather than assuming a full replacement is safe;
- test CDF output and confirm that downstream systems can process the resulting full-snapshot change volume and retention window;
- determine whether dependent materialized views or pipelines incrementally refresh, full-refresh, fail or require a controlled restart;
- validate sharing compatibility, recipient-visible schema and refresh expectations; and
- obtain approval from affected downstream owners for incompatible changes and the cutover/notification plan.

Do not state that replacement universally invalidates all consumers; behaviour is feature- and schema-dependent. Conversely, preservation of the table's Unity Catalog identity does not prove that a stateful downstream reader can safely process a full-data replacement.

### Consumer contract and policy compatibility gates

#### Strategy A: independent-table replacement

Before an independent-table `REPLACE`, inventory the current consumer table's row filters, column masks and applicable governed policies. Compare every referenced column name and type with the candidate schema.

- Fail readiness when a retained row filter or mask would reference a removed or incompatibly changed column.
- Treat a rename of a masked column as an explicit security change. Do not publish until the mask or higher-level policy is deliberately mapped to the new column and independently approved.
- Confirm that columns classified as sensitive in the column contract have the required table-level mask or effective higher-level policy.
- Execute the pre-publication schema/policy inspection using supported compute. The candidate-data query alone is insufficient because a separate candidate table does not inherit the consumer table's retained policies.

The bridge identity only detects and reports incompatibility. Any policy remapping follows the separate governance workflow in §4.

#### Strategy B: release-pointer consistency group

All retained releases for one object share a single physical Delta table schema. Before writing a new release—not merely before changing the pointer—compare the new projection contract with:

- the current physical snapshot-table schema;
- columns referenced by the stable consumer-view definition;
- table-level policies on the physical table, if any; and
- applicable governed policies and required sensitivity classifications.

Do not enable automatic schema evolution on shared release-pointer physical tables. A proposed column removal, rename or incompatible type change requires a separately designed schema migration because altering the shared table can affect the currently published release before the pointer moves. Stop the affected consistency group until the physical table, consumer view and policies are migrated through an approved change.

Choose exactly one approved security-enforcement mode for every release-pointer consistency group and record it in control metadata:

1. **`PHYSICAL_TABLE_POLICY`:** apply the required manual table-level row filter/column masks or effective ABAC policies to the physical snapshot table. The stable consumer view inherits enforcement from its underlying table for the querying session. Prove that the bridge reconciliation identity receives an unfiltered, unmasked validation view of the candidate slice through a narrowly approved UDF branch or ABAC `EXCEPT` rule, and test business-user enforcement separately with the restricted validation identities from §4.
2. **`DYNAMIC_VIEW_SQL`:** keep direct business access to the physical table denied and implement the complete row predicates and masking expressions inside the stable Unity Catalog dynamic-view definition using approved identity functions such as `is_account_group_member()` and `current_user()`/`session_user()`. Treat the view definition and every referenced entitlement table/function as version-controlled security code. Inventory, independently approve and regression-test that code on every change.

Unity Catalog manual row filters, column masks and ABAC policies cannot be attached directly to a standard view. `DYNAMIC_VIEW_SQL` is view SQL—not a table policy object—and must never be represented in the audit as an attached filter, mask or ABAC policy. In both modes, a count observed through consumer filtering is not an authoritative release count. Reject mixed or unrecorded enforcement modes within one consistency group.

Mark a target `READY` only after all mandatory checks pass.

For each release-pointer consistency group, calculate the counts from the immutable enabled inventory, not from the number of slices that happen to have been written:

```text
expected enabled group members
ready enabled group members
failed enabled group members
```

The group is publishable only when:

```text
failed enabled group members = 0
ready enabled group members  = expected enabled group members
```

Additionally verify exactly one validated slice per enabled member for the candidate `release_id` and matching `source_generation_id` and `group_contract_version`. A missing, extra, mismatched or failed slice blocks the entire pointer flip, including a member previously described as optional. The `mandatory_flag` invariant in §1 prevents such a member from entering an enabled group.

If one object fails:

- Keep successful candidate outputs.
- Retry only the failed object.
- Use a new extraction attempt folder through the mapping's approved method.
- Do not publish its consistency group.
- Do not unnecessarily repeat successful extractions.

## 14. Publish to users

### Strategy A: independent-table replacement

For an independent table:

1. Validate the candidate.
2. Pass the consumer policy compatibility gate in §13.
3. Invoke the separate Strategy A publication identity defined in §4. While the orchestrator holds the target's publication lease, conditionally verify that the current release and durable publication high-water sequence still equal the values observed during readiness; pass the validated candidate identity, contract versions and expected values to the publisher through the controlled publication operation. The publisher must recheck them before DDL.
4. Reject the publication unless the candidate sequence is greater than the high-water sequence, except for an explicitly authorised correction/replay tied to a recorded `BAD` release.
5. Resolve the immutable `delta_property_contract_version` approved at readiness and render its complete governed `INDEPENDENT_CONSUMER` property set explicitly in the atomic `CREATE OR REPLACE TABLE ... TBLPROPERTIES (...) AS SELECT ...` statement. Include the immutable release/run metadata in the resulting data and commit audit metadata. Do not assume that an unspecified user-managed property survives replacement merely because table identity, history, grants and supported policies are preserved.
6. Immediately inspect `SHOW TBLPROPERTIES` and `DESCRIBE DETAIL` and compare the effective governed properties, table features, schema and expected Delta version with the approved contracts. A missing, changed or unexpected correctness-sensitive property is a failed publication validation and triggers the assessed rollback/recovery path.
7. Record the new Delta version, release ID, source sequence, property-contract version, rendered-property hash and observed post-replacement property snapshot.
8. Immediately verify retained/effective policies and run the approved queries as the restricted validation identities through supported compute.
9. Run the remaining post-publication checks and release the target lease.

The target lease serialises the readiness recheck and replacement across the orchestrator and the separate publisher. Do not let another owner publish while that operation is in flight. If acknowledgement is lost after replacement, inspect the consumer table's Delta history and embedded release metadata before retrying; do not issue a second replacement blindly.

Maintain one durable publication-state row per independent target with current/previous release IDs, current published source sequence, non-decreasing publication high-water sequence, expected consumer Delta version, control version and publication timestamp. Update that row conditionally while holding the target lease. Because the Delta replacement and a separate control-table update are not one cross-table transaction, recovery must reconcile the consumer table's committed release metadata/history with the state row before another publication is allowed.

### Strategy B: consistency-group release pointer

Maintain one control table keyed by `consistency_group`, containing `published_release_id`, `published_upstream_sequence`, non-decreasing `publication_high_water_sequence`, `previous_release_id`, `control_version` and `published_at`.

Stable consumer views filter their physical snapshot tables using the release published for their group. Change the control row only after every enabled member of the group is `READY` and has an independently validated slice for the candidate release ID.

The new release must already have passed the Strategy B gate in §13 before it was written to the shared physical table. Recheck that the approved contract and policy state has not changed before flipping the pointer.

Publish with one conditional, atomic control-row update while holding the group publication lease. The update must require the expected current `published_release_id`, expected `control_version` and expected high-water value; it must require the candidate `upstream_sequence` to be greater than `publication_high_water_sequence` unless the approved correction path applies. A successful normal publication advances both published sequence and high-water. A zero-row update means another publisher or rollback won the race; stop, reload state and re-evaluate rather than overwriting it. Apply the same rule to an older bridge run that finishes after a newer one.

The pointer lookup adds a control-table predicate to consumer queries. Benchmark representative Power BI and Databricks SQL workloads. Do not replace the pointer with sequential literal view changes when strict group-level consistency is required.

Each query sees whichever committed pointer value it resolves; a sequence of separate queries is not one cross-query snapshot. For reports that must hold one release across all queries, expose an approved parameterised table function or report-specific release dynamic view over the inaccessible physical table and have the consumer resolve one approved `release_id` at refresh start where the tool supports it. Security must follow the group's recorded mode from §13: either underlying physical-table policies that are respected through the function/view, or complete predicate and masking expressions embedded in the dynamic-view/function SQL. Do not attempt to attach manual filters, masks or ABAC policies directly to a standard view, and do not grant direct physical-table access merely to enable pinning. Otherwise schedule the pointer flip inside an agreed quiet window, prevent new affected refreshes during the flip and document that this is an operational control with a residual race—not a database transaction spanning the report.

## 15. Post-publication validation

After publication:

- Query representative consumer objects.
- Confirm business date, source run and release information.
- Run important within-group joins.
- Run critical business totals.
- Confirm Unity Catalog permissions.
- Confirm that expected row filters, column masks and effective governed policies remain attached and enforceable.
- Execute the policy test matrix as the named restricted validation identities; verify both permitted and denied/masked cases.
- Confirm SQL warehouse access.
- Mark freshness `CURRENT`.
- Notify operations of success.

Do not immediately remove the previous validated release or the Delta files required for time travel and rollback.

## 16. Rollback

### Independent replacement table

Before restoring, acquire the target publication lease and verify:

- the recorded previous Delta version and required data files still exist;
- its schema remains compatible with every current row filter, column mask and applicable governed policy;
- the rollback identity and compute support the required time-travel/restore operation; and
- where ABAC applies, the approved rollback identity is in the necessary policy `EXCEPT` scope for the duration of the controlled operation.

Databricks can reject time travel when the historical schema is incompatible with current fine-grained access-control requirements. Treat that condition as a failed automated precheck, not as a reason to drop policies automatically. Invoke the governance-approved recovery path, which may temporarily adjust policy through the governance identity or rebuild from a separately retained, validated prior candidate/recovery copy.

Restore the table to its recorded previous Delta version only after the prechecks pass, then validate schema, counts and policies as both the rollback identity and restricted consumer test identities. Conditionally update the independent target's publication-state row to the restored current release/sequence, but do not reduce its publication high-water sequence. Record the new restore commit/version and release history. Remember that rollback is per table, not atomic across a collection of tables.

### Release-pointer consistency group

1. Mark the faulty release `BAD`.
2. Resolve `previous_release_id` from durable publication history; do not infer it from folder names or maximum dates.
3. Before changing the pointer, verify that every enabled physical snapshot table in the group still contains that release and that its recorded row count/schema match the previously validated release audit.
4. Run a representative direct query against that retained release. If any enabled member is absent or invalid, stop the automated rollback and invoke the documented recovery path instead of moving the pointer into an outage.
5. Atomically move the group's pointer back to `previous_release_id` using a conditional update that checks the faulty current release and expected `control_version`; increment the control version without reducing `publication_high_water_sequence`.
6. Confirm every group consumer returns the previous release.
7. Mark freshness `STALE_ROLLBACK`.
8. Notify users and support.
9. Investigate without immediately deleting the failed release.
10. Create a new release ID for the correction.

Never reuse a bad release ID; allocate a new release identity for the corrected run.

Rollback changes the current published release/sequence but must not reduce `publication_high_water_sequence`. This prevents a delayed older bridge run from becoming eligible merely because the pointer moved backward. A corrected release at an equal or older source sequence requires the explicit, audited correction/replay override described in §7; otherwise create a new upstream run with a higher sequence.

## 17. Retention, `VACUUM` and cleanup

Retain:

- The current validated release.
- The immediately previous validated release.
- Any additional history required by the rollback policy.
- Failed candidates long enough for diagnosis.
- Audit data for the required operational/compliance period.

Configure `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration` to support the rollback and long-running-reader requirements. On tables that use Delta transaction identifiers for retry idempotency, also configure `delta.setTransactionRetentionDuration` as required by §12. Keep log retention at least as long as the approved time-travel window and consistent with the deleted-file retention setting. Table history alone is insufficient if `VACUUM` has already deleted the required data files.

### Size and approve the retention cost

Each full replacement can make one table generation obsolete. Strategy A also writes a separate candidate and then rewrites it into the consumer table; Strategy B writes once into its shared multi-release physical table. Do not describe the whole estate as universally written twice.

For independent replacement tables, estimate retained storage and write I/O using measured compressed Delta bytes per full load, candidate-to-consumer rewrite volume, publication frequency and rollback duration. Include current consumer data, obsolete consumer generations retained for time travel, successful candidates retained for seven days, failed/unpublished candidates, optimized-file rewrites, growth and a safety margin; do not estimate from source row count alone. Record compute and ADLS transaction/egress costs as well as stored bytes.

Shallow clone is not the default publication mechanism. Unity Catalog does not support `CREATE OR REPLACE` over an existing shallow clone, and clone source/lifecycle, permissions, sharing and `VACUUM` behaviour would need a separately proven design. A proposal to write once and rename/swap tables must likewise prove stable object identity, grants, filters/masks, concurrent-reader behaviour and rollback. Retain the candidate-plus-atomic-`REPLACE` design unless a benchmarked alternative passes all of those gates.

Assign retention from `rollback_class`, not automatically from `validation_tier`. Critical business tables may require longer rollback even when their validation workload is small. Record the approved recovery window, estimated steady-state bytes and cost for each class. Alert when actual retained bytes or growth materially exceeds the estimate.

Assume predictive optimization may be enabled for Unity Catalog managed tables. It can automatically run `OPTIMIZE`, `VACUUM` and `ANALYZE`. Verify its effective account, catalog, schema and table setting during deployment.

Set `delta.deletedFileRetentionDuration`, `delta.logRetentionDuration` and, where transaction identifiers are used, `delta.setTransactionRetentionDuration` as part of the applicable versioned property contract before a table becomes eligible for production writes or predictive optimization. Render the complete governed consumer property set during every Strategy A replacement, and verify it immediately afterward as required by §14. For candidate and Strategy B physical tables that are not replaced nightly, verify the applicable property contract after initial creation and on every governed property or feature change. Deleted-file retention must be at least as long as the approved time-travel rollback window and must also cover the longest-running reader. Transaction-identifier retention must cover the bounded retry/recovery horizon plus margin and must not be shortened independently of that recovery policy. The default seven-day deleted-file retention is not sufficient when the approved rollback window is longer; `delta.setTransactionRetentionDuration` has no platform default, so it must be set deliberately when this design relies on it.

Predictive `VACUUM` does not delete data that is still referenced by the current table state. Release-pointer tables should therefore retain the current and approved previous release as live data until the rollback period expires. Independent-table replacement relies on retained historical files, so its rollback window is directly constrained by the configured retention. Monitor automated operations through `system.storage.predictive_optimization_operations_history`.

Do not reduce the standard `VACUUM` safety threshold without a separately approved operating procedure.

Controlled cleanup may remove:

- Expired unpublished Parquet attempts.
- Expired published Parquet staging data.
- Expired independent candidate Delta tables.
- Expired physical snapshot releases.
- Temporary Synapse external-table metadata.
- Expired audit detail.

### Parquet staging retention

Unless a stricter organisational policy is approved, use these bridge defaults:

| Staging state | Retention rule |
|---|---|
| Active bridge run or active CETAS attempt | Never age-delete |
| Successfully published attempt | Eligible for state-aware deletion 7 days after publication |
| Failed or unpublished closed attempt | Eligible for state-aware deletion 30 days after the bridge run closes |
| Attempt under incident, legal or audit hold | Retain until the hold is explicitly released |

Use equivalent explicit defaults for independent candidate tables unless another class is approved:

| Candidate state | Retention rule |
|---|---|
| Owned by an active bridge run | Never delete |
| Successfully published independent candidate | Eligible 7 days after successful post-publication validation |
| Failed or unpublished candidate from a closed run | Eligible 30 days after run closure |
| Candidate referenced by rollback/recovery or under hold | Retain until that reference or hold is explicitly released |

Store the eligibility timestamp and disposition in the object-run audit. Perform guarded, state-aware cleanup from the audit record. An ADLS lifecycle policy may be used only as a conservative backstop on a prefix containing no active attempts, because an age-only rule cannot determine publication or incident state. Account for blob soft-delete retention when estimating storage and confirm that lifecycle rules are scoped to the bridge staging prefix.

Run Parquet deletion only as the staging-cleanup identity defined in §4. Resolve the exact canonical path from the audit record, validate its configured staging-root prefix and object/attempt identity, then conditionally change the disposition from `ELIGIBLE` to `DELETING`. Record deletion outcome and retry safely. The bridge job's read-only staging access must not be silently broadened to make cleanup convenient.

Parquet staging is not the rollback source. After a successful attempt's seven-day staging period expires, rollback continues to use the retained Delta version or retained release-pointer rows governed by `rollback_class`. Re-loading after staging deletion requires a new Synapse extraction. If the organisation requires re-loading without re-extraction, assign a longer successful-staging retention class explicitly and include its cost in capacity planning.

Cleanup must never target:

- A currently published release.
- A retained rollback release.
- An active bridge run.
- An active CETAS attempt folder.
- Files required by the configured Delta retention period.

Before deleting an expired physical release, re-read the publication-control row and durable history under a cleanup lease and prove that the release is neither current nor any retained rollback target. Before dropping an independent candidate table, prove that publication/rollback history does not reference it and that no active run owns it. Candidate retention and diagnostic-hold periods must be defined per state just as Parquet retention is.

After completion, scale down or pause the Synapse pool when that is compatible with the existing operational schedule.

## 18. Observability, alerting and operational response

The Data Operations Owner owns the production dashboard, alert routing, on-call rota and runbook. Platform owners own the Synapse, ADF/Synapse pipeline, ADLS, Azure SQL and Databricks component diagnostics. Every telemetry record must carry the available common identifiers: `upstream_run_id`, `source_generation_id`, `bridge_run_id`, `release_id`, `consistency_group`, `mapping_object_id`, extraction method/attempt, logical-write key, Delta-load attempt and publication control version. Retain this correlation data for at least the audit period defined by organisational policy.

### Required operational status surface

Provide one operator-facing dashboard or query surface sourced from the authoritative control records and platform telemetry. It must show:

- current and most recent bridge run, upstream generation, fence owner/state/age/expiry and execution-lease health;
- counts by phase and terminal state, including expected, queued, running, succeeded, retrying, `READY`, published, failed, blocked and `ABANDONED_GENERATION`;
- per-object extraction method, attempt, size class, start/end/duration, queue time, source request/activity ID, exact staging-path status, source/Parquet/Delta counts and last classified error;
- readiness and publication state by consistency group, including missing or failed enabled members and current/previous/high-water releases;
- p50/p95 phase duration and throughput against the approved SLO, remaining source-fence margin and projected critical-path completion;
- Synapse workload-group assignment/queue pressure, Copy Activity effective DIUs/parallelism, Databricks load concurrency, ADLS throttling and Delta commit/validation outcomes; and
- retention/cleanup backlog, bytes retained by state/class, predictive-optimization activity and failed cleanup operations.

Dashboard convenience must not create a new authority. Links must resolve back to the authoritative Azure SQL, Synapse or Delta row and platform request ID. Detect and display disagreement between control state and platform telemetry as `STATE_DIVERGENCE`, not as success.

### Minimum alerts

Route actionable alerts to the named on-call channel and incident system with severity, identifiers, current owner, failed phase, last error, permitted automated action and runbook link. At minimum alert on:

| Condition | Required response |
|---|---|
| No upstream completion or bridge start by the approved schedule/SLO | Warn, identify the blocking upstream stage and escalate to the upstream owner |
| Fence or lease heartbeat missing, takeover attempted, or remaining fence margin below threshold | Stop new work as applicable and page Data Operations before expiry |
| Fence expired; any §6 fence detector reports a write, generation/state change or mismatching smoke check; or bypass monitoring is indeterminate | Page immediately; block publication and abandon affected generation/release according to §6 |
| Phase p95/SLO breached or projected completion exceeds fence budget | Stop new submissions where required, preserve completed attempts and follow the capacity/fence runbook |
| Mandatory object failed, retries exhausted, or consistency group cannot become ready | Keep the prior release published and alert the owning data/operations teams |
| Parquet/schema/count/Delta reconciliation or policy validation failed | Block publication and page according to data-integrity/security severity |
| Ambiguous Delta commit, duplicate transaction conflict or publication-state divergence | Freeze the affected target/group until history and control state are reconciled |
| Unexpected Synapse workload classifier, sustained queue/resource pressure, integration-runtime saturation or ADLS throttling | Reduce governed concurrency and alert the responsible platform owner |
| Post-publication checks failed or consumer freshness breached | Initiate the assessed rollback decision and notify affected consumers |
| Cleanup backlog/retained bytes exceeds the approved age, count or cost threshold | Alert operations; never bypass state-aware retention guards |
| Telemetry, Synapse audit/diagnostic delivery or dashboard feed is unavailable or late beyond its threshold | Raise a monitoring-health alert; absence of telemetry is not evidence that the fence held or the run succeeded |

Define numeric warning and critical thresholds from measured non-production and parallel-run results before go-live; do not leave “approaching,” “materially exceeds” or “late” as unconfigured prose. Suppress duplicate symptoms under one incident key but never suppress the first integrity, security, fence-loss or publication failure. Emit an explicit successful-run/freshness event only after post-publication and restricted-identity checks pass.

### Runbook and escalation

The version-controlled runbook must cover triage and recovery for every row in §19, including safe retry eligibility, exact evidence to inspect, fence/lease takeover, cancellation, ambiguous commits, state divergence, policy failures, rollback, cleanup failures and immutable evidence preservation. It must identify Data Operations as incident coordinator and name escalation targets for upstream/Synapse, integration runtime, ADLS/network, Databricks, governance/security and downstream consumers. Exercise fence expiry, failed mandatory member, ambiguous commit, policy failure and rollback scenarios before production and at the approved recurring interval. Record exercise results and remediate failed procedures.

## 19. Failure behaviour

| Failure | Required behaviour |
|---|---|
| Upstream run incomplete or still writable | Do not start extraction |
| Source-generation fence cannot be acquired or expires | Stop new source reads; cancel or classify running requests; if another read is required, abandon the affected group generation and restart under a new generation/release as defined in §6 |
| Fence detector reports a write/state-generation change, a source smoke check mismatches, or required bypass monitoring is indeterminate | Stop publication; preserve evidence; close affected attempts as `ABANDONED_GENERATION`; identify the writer or monitoring loss and restart affected groups only under a new closed generation/release |
| Candidate source sequence is not greater than the publication high-water sequence | Reject it as stale unless the explicit audited correction/replay rule applies |
| Execution or publication lease is held by another live owner | Wait or stop; never run a competing publisher |
| Unsupported CETAS type or unsafe LOB/text value | Stop the target; apply an approved cast or change the mapping to the fully configured `COPY_ACTIVITY` route; never fall back implicitly |
| One CETAS operation fails | Retry only that view using a new attempt path |
| One Copy Activity fails | Retry only that mapping through a new attempt path after classifying the failure; prohibit skipped/truncated rows |
| Parquet validation fails | Do not load or publish that target |
| Parquet decode, schema or deterministic conversion fails during Delta write | Retain Parquet; do not retry unchanged input; correct the data or contract first |
| Transient ADLS, compute or Delta commit failure occurs | Retain Parquet and retry under the bounded retry policy |
| Delta commit outcome is ambiguous | Inspect Delta history and the logical write key before retrying; never append blindly |
| `txnAppId`/`txnVersion` pair already exists | Treat it as success only when history, logical identity and the independently queried slice all match; otherwise stop as an integrity conflict |
| Existing Strategy B slice has the same release but a different logical write key | Stop the target as an integrity conflict; do not merge the two attempts |
| Delta reconciliation fails | Do not publish the target or its consistency group |
| Strategy A replacement changes or omits a governed Delta property/table feature | Treat post-publication validation as failed; freeze the target and execute the assessed restore or governed correction path |
| Any enabled release-pointer group member remains failed | Keep the previous group release active |
| Optional independent target fails | Publish other independent targets only if their contracts permit it |
| Schema drift occurs | Stop the affected target/group for contract and consumer-policy compatibility review |
| Fine-grained policy blocks unfiltered validation or historical restore | Stop and invoke the governance-approved validation/rollback path; never disable policy silently |
| ADLS is unavailable or throttled | Pause/retry safely; never publish partial output |
| Post-publication defect occurs | Restore the individual table or move the group pointer back |

## Final operational sequence

1. Complete the existing Synapse ingestion and transformation process.
2. Refresh all one-to-one materialised tables.
3. Record one upstream run ID, row counts and schema versions.
4. Close the upstream run and conditionally acquire `BRIDGE_HELD` through the authoritative source-fence state machine; registered writers must be unable to enter `WRITE_ACTIVE` during CETAS or Copy Activity reads.
5. Confirm the approved network paths, remaining fence budget and authoritative Synapse/Azure SQL/Databricks control state.
6. Create bridge run and release IDs; acquire renewable Azure SQL execution/publication leases and reject sequences that do not advance the durable publication high-water mark.
7. Bind the run to the approved enabled-inventory wave and validate its stable mapping IDs, complete groups, blast-radius approvals, explicit extraction methods and independently calculated schema, projection and Delta-property contract versions.
8. Schedule size-aware CETAS and Copy Activity operations within their tested route-specific and aggregate Synapse concurrency limits; use a manifest only for approved very-large chunked objects.
9. Export explicit presentation-view projections to immutable attempt-specific ADLS paths without mixing methods, attempts, chunks or generations.
10. Validate Parquet structure and independently count every exact leaf path; do not treat that count as an all-column decode test.
11. Decide every extraction/validation retry while the source generation is still fenced. Once all source reads are terminal, all required Parquet attempts are accepted and no retry can reread the old generation, evaluate every fence detector and required source-state smoke check. Release `BRIDGE_HELD` only when no positive or indeterminate result exists; otherwise abandon the affected generation/release as defined in §6. Delta retries from retained Parquet do not extend the fence.
12. For release-pointer targets, pass the shared physical-table contract and policy gate before writing the new release.
13. Load every contracted column into uniquely identified candidate UC managed Delta data with optimized writes, controlled Databricks concurrency and the specified stable transaction-identifier derivation; use successful commit plus history verification as all-column decode evidence.
14. Independently query candidate Delta counts and reconcile source audit, Parquet and Delta; scope every release-pointer check to the exact release/run/object slice and reject duplicates.
15. Run validation-tier business checks and the downstream-consumer compatibility gate.
16. Retry only failed targets from retained Parquet; reacquire a new source generation/release when a source reread is required after fence loss.
17. Validate independent consumer-table policy compatibility, recheck the target's expected current release and publication high-water sequence under its lease, then publish through atomic table replacement with the complete governed `TBLPROPERTIES` contract only when the monotonic rule passes; immediately verify effective properties and table features.
18. Publish ready multi-table consistency groups through a conditional group-pointer update that checks the expected control version/current release/high-water value and advances the high-water mark.
19. Run post-publication queries and the policy matrix as named restricted consumer test identities.
20. Emit the success/freshness event only after the authoritative dashboard shows reconciled platform and control state; otherwise follow the alert and runbook design.
21. Retain the previous state for rollback under explicitly configured and costed Delta retention; test governed rollback prerequisites.
22. Remove expired staging, candidates and releases only through the designated cleanup identity and guarded, state-aware cleanup.
23. Scale down or pause Synapse when operationally permitted.

## Minimum implementation artifacts

The production repository must contain the following version-controlled artifacts. The fragments below define required behaviour; deployment code must substitute approved identifiers through safe parameterisation/identifier rendering and must not concatenate unrestricted mapping text.

### Azure SQL control constraints

At minimum, enforce these keys in DDL:

```text
bridge_mapping:
  PRIMARY KEY (mapping_object_id)
  UNIQUE (source_schema, source_view)
  UNIQUE (target_catalog, target_schema, target_table)

projection_contract:
  PRIMARY KEY (mapping_object_id, projection_contract_version)
  columns include approval_status, approved_by, approved_at and contract_hash

column_contract:
  PRIMARY KEY (mapping_object_id, projection_contract_version, ordinal)
  UNIQUE (mapping_object_id, projection_contract_version, normalized_target_column)
  FOREIGN KEY (mapping_object_id, projection_contract_version)
    REFERENCES projection_contract

delta_property_contract_header:
  PRIMARY KEY (mapping_object_id, delta_property_contract_version)
  columns include approval_status, approved_by, approved_at and contract_hash

delta_property_contract:
  PRIMARY KEY (mapping_object_id, delta_property_contract_version, asset_role, property_name)
  FOREIGN KEY (mapping_object_id, delta_property_contract_version)
    REFERENCES delta_property_contract_header

compatibility_assessment:
  PRIMARY KEY (mapping_object_id, compatibility_assessment_id)
  columns include extraction_method, source_schema_version,
                  projection_contract_version, connector_runtime_version,
                  settings_hash, assessment_result, approved_by and approved_at

bridge_inventory_version:
  PRIMARY KEY (inventory_version)
  columns include approval_status, approved_by, approved_at and control_version

consistency_group_assignment:
  PRIMARY KEY (group_contract_version, mapping_object_id)
  columns include consistency_group, consistency_status, approval_status, owner_id,
                  evidence_reference, blast_radius_result and availability_acceptance

bridge_inventory_member:
  PRIMARY KEY (inventory_version, mapping_object_id)
  FOREIGN KEY (inventory_version) REFERENCES bridge_inventory_version
  FOREIGN KEY (mapping_object_id) REFERENCES bridge_mapping
  FOREIGN KEY (group_contract_version, mapping_object_id)
    REFERENCES consistency_group_assignment
  FOREIGN KEY (mapping_object_id, compatibility_assessment_id)
    REFERENCES compatibility_assessment
  FOREIGN KEY (mapping_object_id, projection_contract_version)
    REFERENCES projection_contract
  FOREIGN KEY (mapping_object_id, delta_property_contract_version)
    REFERENCES delta_property_contract_header
  columns include enabled_flag, group_contract_version, extraction_method,
                  compatibility_assessment_id, publication_strategy,
                  security_enforcement_mode, mandatory_flag,
                  expected_source_schema_version, projection_contract_version,
                  delta_property_contract_version, validation_tier, business_key,
                  key_normalization_rule, allow_empty, expected_min_rows,
                  datetime_policy, datetimeoffset_policy, float_tolerance,
                  rollback_class, staging_retention_class, chunking_strategy,
                  chunk_key, size_category and column_mapping_mode

bridge_run:
  PRIMARY KEY (bridge_run_id)
  FOREIGN KEY (inventory_version) REFERENCES bridge_inventory_version
  columns include upstream_run_id, source_generation_id, release_id, status,
                  created_at_utc, completed_at_utc and control_version

source_generation_fence:
  PRIMARY KEY (source_set_id)
  UNIQUE (source_generation_id)
  columns include state, writer_owner_id, bridge_owner_id, expires_at_utc,
                  heartbeat_at_utc and control_version

source_fence_detection_event:
  PRIMARY KEY (detection_event_id)
  FILTERED UNIQUE INDEX (event_source, source_event_id)
    WHERE source_event_id IS NOT NULL
  columns include event_source, nullable source_event_id, source_set_id,
                  source_generation_id, bridge_run_id, event_time_utc,
                  detection_type, principal_id, affected_object,
                  classification and disposition

bridge_extraction_attempt:
  PRIMARY KEY (bridge_run_id, mapping_object_id, extraction_attempt)
  UNIQUE (canonical_attempt_path)

bridge_logical_write:
  PRIMARY KEY (mapping_object_id, release_id)
  UNIQUE (mapping_object_id, txn_app_id, txn_version)

bridge_delta_load_attempt:
  PRIMARY KEY (bridge_run_id, mapping_object_id, extraction_attempt, delta_load_attempt)
  FOREIGN KEY (bridge_run_id, mapping_object_id, extraction_attempt)
    REFERENCES bridge_extraction_attempt
  FOREIGN KEY (mapping_object_id, release_id)
    REFERENCES bridge_logical_write

extraction_chunk:
  PRIMARY KEY (bridge_run_id, mapping_object_id, extraction_attempt, chunk_id)
  UNIQUE (canonical_chunk_path)

bridge_lease:
  PRIMARY KEY (lease_key)
  columns include owner_id, expires_at_utc, heartbeat_at_utc and control_version
```

Store `normalized_target_column` as the approved case-insensitive canonical form used for duplicate detection. `bridge_extraction_attempt` owns the immutable Parquet path, `bridge_logical_write` owns the stable transaction pair, and `bridge_delta_load_attempt` records each submission/recovery attempt by reference. A Delta retry from retained Parquet therefore creates a new Delta-attempt row without duplicating either unique value. Foreign keys should also connect mappings, contracts, runs and chunks; use check constraints for allowed states/methods and positive attempt numbers. Exact DDL depends on the organisation's naming, temporal-history and retention standards.

### Conditional lease/state template

```sql
BEGIN TRANSACTION;

UPDATE control.bridge_lease
SET owner_id = @owner_id,
    expires_at_utc = @new_expiry_utc,
    heartbeat_at_utc = SYSUTCDATETIME(),
    control_version = control_version + 1
WHERE lease_key = @lease_key
  AND control_version = @expected_control_version
  AND (owner_id = @owner_id OR expires_at_utc < SYSUTCDATETIME());

-- Require exactly one affected row; otherwise roll back and reload state.

COMMIT TRANSACTION;
```

Initial lease-row creation must handle unique-key conflict as “another owner won,” not as an instruction to overwrite. Equivalent expected-state/expected-version predicates are required for every run, attempt, approval, cleanup and publication transition.

### CETAS rendering template

```text
CREATE EXTERNAL TABLE <quoted unique metadata schema/table>
WITH (
  LOCATION = '<validated relative attempt path>',
  DATA_SOURCE = <pre-created quoted Hadoop data source>,
  FILE_FORMAT = <pre-created quoted Parquet file format>
)
AS
SELECT <ordered allow-listed projection rendered from one contract version>
FROM <quoted source schema>.<quoted presentation view>
<optional approved deterministic chunk predicate>;
```

The renderer must validate the location beneath the staging root, ensure it is empty, quote every identifier, bind only approved expression templates, add the run/object query label and save the final rendered statement hash/text in restricted audit storage. A `finally` path drops only the exact temporary external-table metadata name.

### Copy Activity deployment template

```text
Inputs: mapping_object_id, bridge_run_id, release_id, source_generation_id,
        extraction_attempt, projection_contract_version, exact sink leaf path
Source: explicit ordered query generated by the same contract renderer
Sink: Parquet; no schema inference drift; no truncation; no skipped rows
Capacity: integration_runtime, data_integration_units_or_auto, parallel_copies,
          source_partition_policy, size_class_concurrency, aggregate_source_budget
Audit: pipeline_run_id, activity_run_id, rows_read, rows_written,
       files_written, bytes_written, effective_DIUs/parallelism,
       queue/start/end/status/error
Retry: new immutable attempt path; never reuse the failed path
```

### Databricks loader skeleton

```text
1. Read only the audited attempt/chunk manifest paths with the explicit schema.
2. Validate exact names, ordinals, types and configured calendar/timestamp rules.
3. Count Parquet independently.
4. Acquire/verify the logical Delta write key and stored txn pair.
5. Write every contracted field atomically to the unique candidate or release slice.
6. Inspect Delta history and embedded commit metadata.
7. Query the committed candidate/slice independently and reconcile it.
8. Run business, downstream-consumer and policy gates.
9. Conditionally mark READY; publication is a separate leased transition.
```

### Deployment acceptance evidence

Before production, retain evidence for:

- enforced Azure SQL constraints and concurrency tests, including multiple Delta attempts referring to one extraction and logical write;
- the registered-writer inventory, conditional source-fence exclusion, takeover/expiry and break-glass-abandonment tests; a deliberate test event for every fence detector; and proof that an unavailable/delayed Synapse audit path produces an indeterminate result rather than false success;
- approved consistency-group evidence, transitive grouping results, blast-radius threshold evaluation, availability acceptance, escalation decisions and representative multi-query refresh/failure tests;
- staged-enablement tests proving that an approved complete wave can publish while unreviewed mappings remain disabled, and that `UNASSIGNED`, partially enabled or mid-run inventory changes are rejected;
- successful, failed and partially cleaned CETAS paths;
- Copy Activity failure/retry tests and the route-specific/combined capacity ladder, including effective DIUs, parallel copies, integration-runtime saturation and source impact;
- ancient-date, daylight-saving and `time(p)` boundary/precision tests under the approved target representation;
- case-only and special-character columns;
- ambiguous Delta commit recovery within the configured transaction-retention horizon and duplicate transaction-pair handling;
- exact runtime/warehouse support for every protected-table read and write command;
- the exact Strategy A replacement statement succeeding under the separate publication identity with `MANAGE` on its target and `CREATE TABLE` on the containing schema, including an initial table and a governance-owned existing table; a negative test that the loading identity cannot replace or change the consumer table's policies;
- a group containing an enabled `mandatory_flag = false` mapping being rejected; an absent, failed, extra or wrong-generation enabled slice blocking the pointer flip; and rollback confirming all enabled prior-release slices exist;
- successful execution of the gate's exact `SHOW POLICIES`, `SHOW EFFECTIVE POLICIES`, `DESCRIBE POLICY` and information-schema inspections by the read-only bridge identity;
- Strategy A policy preservation plus property-contract rendering and post-replacement `SHOW TBLPROPERTIES`/`DESCRIBE DETAIL` verification;
- each Strategy B group's approved `PHYSICAL_TABLE_POLICY` or `DYNAMIC_VIEW_SQL` mode and restricted-user test results;
- downstream stream/CDF/pipeline/share tests and Strategy B multi-query behaviour;
- rollback after retention/`VACUUM`, network/firewall/private-DNS tests and measured critical-path/storage-cost results; and
- dashboard state reconciliation, monitoring-health detection, alert routing and completed on-call runbook exercises.

## Official references

- [Microsoft: CREATE EXTERNAL TABLE AS SELECT (CETAS)](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-table-as-select-transact-sql)
- [Microsoft: CREATE MASTER KEY](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-master-key-transact-sql)
- [Microsoft: Managed-identity external-table prerequisites](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/tutorial-external-tables-using-managed-identity)
- [Microsoft: Dedicated SQL pool server- and database-level permissions](https://learn.microsoft.com/en-us/sql/t-sql/statements/permissions-grant-deny-revoke-azure-sql-data-warehouse-parallel-data-warehouse)
- [Microsoft: Microsoft Entra server logins, including service principals](https://learn.microsoft.com/en-us/azure/azure-sql/database/authentication-azure-ad-logins-tutorial)
- [Microsoft: External tables in dedicated and serverless Synapse SQL pools](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-external-tables)
- [Microsoft: CETAS migration guidance](https://learn.microsoft.com/en-us/fabric/data-warehouse/migration-synapse-dedicated-sql-pool-methods#migration-of-data-with-cetas)
- [Microsoft: Memory and concurrency limits in dedicated SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/memory-concurrency-limits)
- [Microsoft: Data types in Synapse dedicated SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-data-types)
- [Microsoft: Copy and transform data in Azure Synapse Analytics](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-sql-data-warehouse)
- [Microsoft: Copy Activity performance features](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-performance-features)
- [Microsoft: Auditing for Azure SQL Database and Azure Synapse Analytics](https://learn.microsoft.com/en-us/azure/azure-sql/database/auditing-overview)
- [Databricks: CREATE TABLE / CREATE OR REPLACE TABLE](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using)
- [Databricks: Drop or replace a table](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/drop-table)
- [Databricks: TIMESTAMP type](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/timestamp-type)
- [Databricks: TIMESTAMP_NTZ type](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/timestamp-ntz-type)
- [Databricks: TIME type](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/time-type)
- [Databricks: OPTIMIZE](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-optimize)
- [Databricks: Delta Lake best practices](https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices)
- [Databricks: Predictive optimization for Unity Catalog managed tables](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization)
- [Databricks: Manually apply row filters and column masks](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/filters-and-masks/manually-apply)
- [Databricks: Dynamic views for row filtering and masking](https://learn.microsoft.com/en-us/azure/databricks/views/dynamic)
- [Databricks: ABAC requirements and view limitations](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/requirements)
- [Databricks: Create and inspect ABAC policies](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/policies)
- [Databricks: Delta table properties, including transaction retention](https://learn.microsoft.com/en-us/azure/databricks/tables/table-properties)
- [Databricks: SHOW POLICIES and read-only policy metadata](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-show-policies)
- [Databricks: DESCRIBE POLICY](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-describe-policy)
- [Databricks: COLUMN_MASKS information schema](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/information-schema/column_masks)
- [Databricks: Access Connector managed identities for Unity Catalog storage](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities)
- [Databricks: Lakeflow Jobs identities and permissions](https://learn.microsoft.com/en-us/azure/databricks/jobs/privileges)
- [Databricks: Unity Catalog privileges, including READ FILES and WRITE FILES](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/access-control/privileges-reference)
- [Databricks: Idempotent Delta writes with transaction identifiers](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/delta-lake)
- [Databricks: ABAC policy evaluation and recovery-operation limitations](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/policy-evaluation)
- [Databricks: Fine-grained access-control time-travel errors](https://learn.microsoft.com/en-us/azure/databricks/error-messages/error-classes)
- [Databricks: Dynamic file pruning](https://learn.microsoft.com/en-us/azure/databricks/optimizations/dynamic-file-pruning)
- [Microsoft: Workload classification for dedicated SQL pool](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-workload-classification)
- [Apache Spark: Parquet data source](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Microsoft: Azure Blob Storage lifecycle management](https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview)
- [Microsoft: Dedicated SQL pool table constraints](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-table-constraints)
- [Microsoft: Dedicated SQL pool transaction isolation](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-develop-transactions)
- [Databricks: Delta column mapping](https://learn.microsoft.com/en-us/azure/databricks/tables/features/column-mapping)
- [Databricks: Shallow clone for Unity Catalog tables](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/clone-unity-catalog)
- [Databricks: Schema-update impact on streams](https://learn.microsoft.com/en-us/azure/databricks/tables/update-schema)
- [Databricks: Create and regionally assign a Unity Catalog metastore](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-metastore)
- [Microsoft: Create ADLS Gen2 storage with hierarchical namespace](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
