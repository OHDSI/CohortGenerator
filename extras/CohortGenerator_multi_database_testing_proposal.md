# CohortGenerator Multi-Database Testing Proposal and Branch Status

## Purpose

This document records the multi-database testing design prototyped on the `update-database-testing-approach` branch, the progress made on that branch, and the work that remains before the design should be adopted across HADES.

The reusable-package extraction plan is maintained separately in [HADES_multi_database_testing_package_handoff.md](HADES_multi_database_testing_package_handoff.md).

## Status Snapshot

Repository state inspected for this update:

- branch: `update-database-testing-approach`;
- branch commit before this documentation update: `ba5069d`;
- comparison base: `develop` at `fa42dd7`;
- snapshot date: 2026-10-18.

The original proposal has advanced from design into a working CohortGenerator prototype. The branch now has a declarative platform file, one-DBMS-per-process selection, a focused live-database test, CI-specific failure behavior, and a separate matrix workflow for external databases.

The prototype is not yet a complete implementation of the declaration contract. Several YAML fields are not enforced at runtime, the workflow does not honor the declared CI tiers, and the BigQuery runner conflicts with the current platform restriction. Remote workflow outcomes and live credentials were not verified as part of this documentation update.

## Design Principles

The prototype and next implementation should continue to follow these principles:

- `DatabaseConnector` is the database access layer.
- Tests target existing live database servers with an available OMOP CDM.
- CDM provisioning is outside the package test run.
- One R process targets one DBMS.
- The same package behavior test runs against every selected DBMS.
- Ordinary package checks do not receive live database credentials.
- Local missing credentials cause a clear skip.
- Required CI database jobs fail when their environment is incomplete.
- Configuration contains metadata and environment-variable names, never secret values.
- Package support declarations and CI execution must not drift silently.

## Progress at a Glance

| Original proposal item | Branch status | Follow-up |
|---|---|---|
| Package-level YAML declaration | Implemented | Make every field executable and fully validate the schema |
| `yaml` suggested dependency | Implemented | Move generic dependency to the shared package after extraction |
| One DBMS selected by `HADES_TEST_DBMS` | Implemented | Validate selection through the declaration in the runtime path |
| Explicit CI mode via `HADES_DATABASE_TEST` | Implemented | Combine it with the selected platform's `required` policy |
| Local skip versus CI failure | Partially implemented | Validate before constructing connection details; honor optional platforms |
| Database test context | Implemented as a named list | Stabilize and move the generic contract to the shared package |
| Platform-specific helper decomposition | Implemented | Replace conditionals with tested internal adapters in the shared package |
| One database-neutral behavior test | Implemented | Keep it in CohortGenerator during extraction |
| Separate live-database workflow | Implemented | Align runners and declared CI tiers |
| Named matrix job per DBMS | Implemented | Derive or validate the matrix against YAML |
| Remove secrets from normal checks | Implemented | Preserve this separation |
| GitHub environment per DBMS | Implemented | Document and validate environment/secret mapping |
| Full config validation | Not implemented | Add field, type, uniqueness, and cross-field validation |
| Matrix/config drift check | Not implemented | Provide shared validation or matrix generation |
| Normalized `HADES_*` credential variables | Not implemented | Add in the shared package with legacy aliases |
| Pull-request versus scheduled tiers | Declared but not enforced | Split triggers/jobs or filter a generated matrix |
| Reusable HADES package/workflow | Not implemented | Follow the handoff plan |

## Current Branch Architecture

The implemented flow is:

```text
inst/test-config/hades-database-platforms.yml
                    |
HADES_TEST_DBMS selects one external DBMS
                    |
tests/testthat/helper-database-platforms.R
resolves environment, connection details, and schemas
                    |
tests/testthat/helper.R::getPlatformConnectionDetails()
downloads drivers and applies SqlRender options
                    |
tests/testthat/test-dbms-platforms.R
runs CohortGenerator behavior and cleanup
```

In CI, `.github/workflows/database-platform-tests.yml` supplies `HADES_TEST_DBMS` and `HADES_DATABASE_TEST=true` to one matrix job per external DBMS.

## Implemented Repository Changes

### Platform declaration

The branch adds:

```text
inst/test-config/hades-database-platforms.yml
```

The declaration identifies CohortGenerator as `databaseConnection: all` and lists:

| DBMS | Enabled | Required | Declared CI tier | Connection mode |
|---|---:|---:|---|---|
| SQLite | Yes | No | `none` | `local` |
| PostgreSQL | Yes | Yes | `pull-request` | `server` |
| SQL Server | Yes | Yes | `pull-request` | `server` |
| Oracle | Yes | Yes | `pull-request` | `server` |
| Redshift | Yes | Yes | `scheduled` | `server` |
| Spark | Yes | Yes | `scheduled` | `connectionString` |
| BigQuery | Yes | Yes | `scheduled` | `connectionString` plus key file |
| Snowflake | Yes | Yes | `scheduled` | `connectionString` |
| IRIS | Yes | Yes | `scheduled` | `connectionString` |

The declaration also records the environment prefix, CDM version, and whether SqlRender temporary-table emulation is expected.

Secret values, servers, connection strings, and schema values remain outside YAML.

### Configuration loading and selection

`tests/testthat/helper-database-platforms.R` now provides:

```r
getDatabaseTestConfig()
getSelectedTestDbms()
getDatabasePlatformConfig()
skipIfNoLiveDatabase()
isCiDatabaseTest()
skipIfLiveDatabaseTest()
```

Configuration is loaded from the installed package first and falls back to the source tree. The loader currently validates:

- schema version 1;
- package name `CohortGenerator`; and
- `databaseConnection` in `none`, `subset`, or `all`.

The selected DBMS is normalized from `HADES_TEST_DBMS` by trimming whitespace and converting to lower case.

The source-tree fallback is relative to `getwd()`. It works from the repository root but is not robust when direct testthat helpers change the working directory.

### Environment validation

The branch maps required legacy environment variables for all eight external DBMS platforms. Missing variables:

- produce a `testthat` skip outside explicit live CI; and
- produce an error when `HADES_DATABASE_TEST=true`.

This is a meaningful improvement over silently printing that a platform is unavailable. However, the runtime currently validates after context construction and does not consult the YAML `required` field.

### Platform adapters

The large historical connection helper has been divided into platform-specific functions for:

- required environment variables;
- `DatabaseConnector::createConnectionDetails()` calls;
- CDM, vocabulary, and writable schemas;
- temporary-table emulation;
- driver requirements; and
- operating-system restrictions.

The current prototype preserves the legacy environment names used by CohortGenerator's GitHub secrets.

Two explicitly temporary connection-string workarounds are present:

- Spark adds `EnableArrow=0` when missing.
- Snowflake adds `JDBC_QUERY_RESULT_FORMAT=JSON` when missing.

These should be isolated behind shared platform adapters with clear removal criteria.

### Database test context

`getDatabaseTestContext()` returns the common settings needed by the behavior test:

```r
list(
  dbmsPlatform = dbmsPlatform,
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  needsWindowsOnly = needsWindowsOnly
)
```

External platforms receive a generated cohort table name to reduce collision risk. SQLite continues to use Eunomia and the `main` schema in ordinary tests.

### Focused live-database execution

`tests/testthat/test-dbms-platforms.R` no longer loops through a hard-coded vector. It reads the single selected platform and runs the existing CohortGenerator behavior test for that platform.

The behavior test continues to verify cohort generation, cohort counts, and negative-control cohort counts. Package-specific table cleanup remains in the test through `on.exit()`.

Most ordinary test files now begin with:

```r
skipIfLiveDatabaseTest()
```

This allows the live workflow to use `R CMD check` while focusing runtime on the DBMS behavior test. `tests/testthat/setup.R` also avoids constructing the Eunomia fixture when a live DBMS is selected.

### Unit coverage for the infrastructure

`tests/testthat/test-database-platforms.R` covers:

- the declared platform list;
- selected-platform normalization;
- missing and unknown selections;
- representative environment-variable mappings;
- local skip behavior;
- CI failure behavior;
- context assembly;
- BigQuery operating-system policy;
- Spark connection-string normalization; and
- Snowflake connection-string normalization.

### GitHub Actions separation

The normal `.github/workflows/R_CMD_check_Hades.yaml` no longer receives live database credentials.

The new `.github/workflows/database-platform-tests.yml`:

- runs on matching pull requests, matching pushes, and manual dispatch;
- uses a static eight-platform matrix;
- names each check `Database test (<dbms>)`;
- uses `fail-fast: false`;
- assigns a DBMS-specific GitHub environment;
- sets `HADES_DATABASE_TEST=true`;
- sets `HADES_TEST_DBMS` to the matrix DBMS; and
- runs `R CMD check` on Ubuntu.

This achieves job isolation and clearer failure names. It does not yet implement the tiers written in the YAML file.

## Gaps and Known Problems

### The configuration is descriptive rather than authoritative

The runtime does not currently use most platform fields from YAML. In particular, connection construction does not enforce `enabled`, `required`, `ci`, `connectionMode`, `credentialMode`, `cdmVersion`, or `tempEmulationSchema` from the selected platform record.

The next implementation should either use each documented field or reject it. Parallel hard-coded policy in R and YAML will drift.

### Environment validation occurs after construction begins

The current wrapper obtains the database test context before calling `validateDatabaseTestEnvironment()`. That means incomplete values can reach `createConnectionDetails()` or temporary credential handling before the intended skip/error.

Validation must precede all connection construction and driver downloads.

### The YAML `required` flag is ignored

Any missing environment variable errors in explicit live CI. Optional-platform behavior is therefore not implemented even though the declaration can express it.

### CI tier declarations are not enforced

PostgreSQL, SQL Server, and Oracle are marked `pull-request`; Redshift, Spark, BigQuery, Snowflake, and IRIS are marked `scheduled`. The workflow currently runs all eight for both pushes and pull requests, and it does not define a schedule.

The workflow should derive jobs from the declaration or validate a static matrix against the selected tier.

### BigQuery is selected on an incompatible runner

The helper treats BigQuery as Windows-only, but the live workflow runs every matrix entry on Ubuntu. The helper can return `NULL`, after which the test prints that no details are available. A required job can therefore finish without exercising BigQuery.

Required live jobs must fail when the selected platform is incompatible with the runner, and the matrix must give BigQuery a supported runner.

### A green required job does not prove a connection attempt

The target contract is stronger than “the test file did not error.” A required job should record that it constructed a valid context and attempted a live connection before package behavior runs. Unsupported-runner paths and other early returns must not appear successful.

### Matrix and declaration can drift

The DBMS list exists in both YAML and the workflow. No test currently compares them.

### Configuration validation is incomplete

Still needed:

- required platform fields;
- field types;
- canonical and unique DBMS names;
- allowed CI tiers and connection modes;
- `reason` for disabled platforms;
- valid combinations of credential and connection modes; and
- consistency between `databaseConnection` and platform entries.

### Source-tree configuration discovery is working-directory dependent

The development fallback assumes `getwd()` is the package root. A direct `testthat::test_dir()` call changes the working directory and can make the loader search below `tests/testthat/inst`. The shared implementation should accept an explicit root or use package-aware discovery that works from both source and installed tests.

### The public environment contract is not normalized

Only existing `CDM5_*` and `CDM_*` variables are implemented. This preserves branch compatibility but leaves shared helpers aware of naming exceptions. A reusable package should prefer normalized `HADES_*` variables and accept explicit legacy aliases during migration.

### Global option lifecycle needs isolation

The branch sets `sqlRenderTempEmulationSchema` globally. Shared tooling should scope and restore it with `withr` even when a test fails.

### Fork pull requests remain unresolved

Forked pull requests cannot safely receive repository or environment secrets. The project still needs a trusted execution policy such as manual dispatch, a protected maintainer branch, or another explicitly authorized workflow.

## Updated Target Architecture

The CohortGenerator prototype should evolve into two layers:

```text
Shared HADES test package
  - schema and config validation
  - selection and strictness policy
  - environment resolution
  - platform adapters
  - standard database test context
  - testthat helpers
  - CI matrix/workflow support
                 |
                 v
CohortGenerator
  - platform declaration
  - GitHub environment selection
  - package behavior test
  - package database-object cleanup
```

This keeps the cross-package contract centralized without moving package-specific assertions into generic infrastructure.

The detailed API, phases, pull-request sequence, and acceptance criteria are in `extras/HADES_multi_database_testing_package_handoff.md`.

## Revised Configuration Contract

The current schema is a useful starting point. Before reuse, the following semantics should become normative:

### Top-level fields

- `schemaVersion`: configuration schema version.
- `package`: package that owns the declaration.
- `databaseConnection`: `none`, `subset`, or `all`.
- `platforms`: unique platform records, omitted or empty for `none`.

### Platform fields

- `dbms`: canonical `DatabaseConnector` identifier.
- `enabled`: whether the package currently claims and tests support.
- `required`: whether absence or failure is allowed in the selected CI tier.
- `ci`: automation tier.
- `cdmVersion`: expected OMOP CDM version.
- `connectionMode`: local, server, or connection string.
- `credentialMode`: optional special credential handling.
- `tempEmulationSchema`: whether a work schema must be configured for SqlRender emulation.
- `reason`: required for a disabled platform.
- runner compatibility: add an explicit field or central shared-package policy.

`environmentPrefix` should not be treated as sufficient to derive every legacy variable name. Current conventions include exceptions that should be expressed by normalized workflow mappings or explicit alias maps.

## Revised Execution Contract

For a selected platform, the shared implementation should execute these steps in order:

1. locate and read the declaration;
2. fully validate the declaration;
3. normalize and validate `HADES_TEST_DBMS`;
4. resolve the selected platform record;
5. enforce `enabled`, CI tier, required status, and runner compatibility;
6. resolve normalized environment variables and legacy aliases;
7. validate required values without exposing them;
8. prepare temporary credentials and scoped options;
9. create `DatabaseConnector` connection details;
10. optionally verify a live connection;
11. return the standard context;
12. run the consuming package's behavior test; and
13. clean up package objects, temporary files, and process state on success or failure.

## Local Developer Workflow

The current branch supports explicit selection with legacy environment variables.

PowerShell example:

```powershell
$env:HADES_TEST_DBMS = "postgresql"
$env:HADES_DATABASE_TEST = "false"
Rscript -e 'devtools::test(filter = "dbms-platforms", reporter = "summary")'
```

Expected behavior:

- no `HADES_TEST_DBMS`: the live behavior test skips;
- selected DBMS with incomplete local variables: the test skips and lists missing variable names;
- selected DBMS with complete variables: only that platform is exercised; and
- explicit live CI mode with incomplete variables: the check fails.

After extraction, the command should remain stable while credential inputs transition to normalized `HADES_*` variables.

## Recommended Next Step

Do not continue expanding copied CohortGenerator helpers. Create the shared package and implement the configuration contract first.

The first shared-package pull request should:

1. scaffold the package and ordinary checks;
2. add a representative configuration fixture;
3. implement configuration discovery, parsing, and complete validation;
4. define the database test context contract;
5. test configuration and CI-tier semantics without credentials; and
6. leave live connection adapters for the next pull request.

Once that contract is reviewed, port PostgreSQL as the first adapter and migrate CohortGenerator incrementally.

## Completion Criteria for the CohortGenerator Prototype

Before this branch is considered a complete reference implementation:

- all YAML fields used in the declared contract are enforced;
- environment validation occurs before connection construction;
- `required` and CI tiers affect behavior as documented;
- the workflow has a scheduled path for scheduled platforms;
- each DBMS uses a compatible runner;
- required jobs cannot pass without a connection attempt;
- the matrix is derived from or validated against YAML;
- global SqlRender options are restored;
- temporary credential material is always deleted;
- live workflow behavior is verified for every declared external DBMS;
- fork pull-request behavior is documented; and
- the generic implementation has a migration path into the shared HADES package.

## Scope Boundaries

The following remain out of scope for the first shared implementation:

- automatic database or CDM provisioning;
- SQL line coverage;
- CDM content-equivalence certification;
- database performance benchmarks;
- database-version compatibility ranges;
- organization-wide secret rotation; and
- automatic cleanup of consumer-owned database objects.

The immediate goal is narrower: establish one trustworthy, reusable contract for declaring, selecting, configuring, and running live database behavior tests across HADES.
