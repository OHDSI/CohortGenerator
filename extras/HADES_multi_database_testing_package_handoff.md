# HADES Multi-Database Testing Package Handoff

## Handoff Summary

This document is the implementation handoff for extracting the reusable live-database testing concepts prototyped in CohortGenerator into a separate R package for HADES.

Working package name: `HadesTest`.

The name is intentionally provisional. Confirm the repository and package name with HADES maintainers before creating the repository. The proposed scope is database-test infrastructure, not a general replacement for `testthat`.

Branch snapshot used for this handoff:

- repository: `OHDSI/CohortGenerator`;
- branch: `update-database-testing-approach`;
- branch commit before the documentation update: `ba5069d`;
- comparison base: `develop` at `fa42dd7`;
- snapshot date: 2026-10-18.

The branch is a useful prototype, but it should be treated as discovery code rather than copied directly into the new package. It proves the execution model while also exposing several contracts that need to be made explicit and tested centrally.

## Goal

Create a small R package that gives HADES packages one standard way to:

1. declare the databases they support and test;
2. select one DBMS per R process;
3. resolve and validate credentials and schemas;
4. construct a consistent `DatabaseConnector` test context;
5. distinguish an ordinary test skip from a required CI failure;
6. isolate package checks from live-database checks;
7. derive or validate CI jobs from the same declaration; and
8. run package-owned behavioral tests against that context.

The package should standardize test infrastructure. It should not contain CohortGenerator behavior, provision OMOP CDMs, or own organization secrets.

## What the CohortGenerator Branch Proves

The branch implements the following concepts:

- a package-level declaration at `inst/test-config/hades-database-platforms.yml`;
- `databaseConnection` values of `none`, `subset`, and `all`;
- a nine-platform declaration: SQLite plus eight external DBMS platforms;
- selection of one platform with `HADES_TEST_DBMS`;
- explicit live-test intent with `HADES_DATABASE_TEST`;
- local skip versus CI failure behavior for missing environment variables;
- platform-specific connection and schema resolution;
- a common database test context passed to package behavior tests;
- collision-resistant cohort table names;
- temporary BigQuery key-file handling;
- SqlRender temporary-table emulation settings;
- a separate GitHub Actions matrix with one named job per external DBMS;
- removal of live database secrets from the ordinary `R CMD check` workflow;
- opt-out of ordinary test files while the focused live-database check runs; and
- unit tests for configuration loading, selection, environment validation, context assembly, and connection-string normalization.

The external matrix currently names PostgreSQL, SQL Server, Oracle, Redshift, Spark, BigQuery, Snowflake, and IRIS. SQLite remains the local database used by the ordinary package tests.

## Extraction Boundary

### Move into the shared package

The shared package should own:

- locating, reading, and fully validating the package declaration;
- canonical DBMS names and aliases;
- selected-platform resolution;
- enabled, required, and CI-tier semantics;
- normalized environment-variable resolution;
- optional legacy environment-variable aliases;
- local-skip versus required-CI-failure policy;
- JDBC driver-folder resolution;
- platform adapters that create `DatabaseConnector` connection details;
- BigQuery temporary credential-file lifecycle;
- platform compatibility checks;
- SqlRender test-option setup and restoration;
- unique database object-name generation;
- a stable database test context contract;
- testthat-facing skip helpers;
- CI matrix derivation and declaration/workflow drift validation; and
- reusable workflow templates or a reusable GitHub workflow.

### Keep in each consuming package

Each HADES package should continue to own:

- the declaration of platforms it supports;
- its live-database behavioral test;
- package-specific fixture creation;
- package-specific database objects and cleanup;
- assertions describing successful package behavior;
- any package-specific platform exclusion, with a documented reason; and
- the small caller workflow that chooses triggers and GitHub environments.

### Keep outside the shared package

The first release should not own:

- database or OMOP CDM provisioning;
- GitHub environment creation;
- secret values or secret rotation;
- shared writable-schema allocation;
- SQL coverage instrumentation;
- performance benchmarking;
- database-version certification; or
- arbitrary package test orchestration unrelated to live databases.

## Current Prototype Assets and Their Destinations

| CohortGenerator asset | Concept to retain | Destination |
|---|---|---|
| `inst/test-config/hades-database-platforms.yml` | Machine-readable support and test declaration | Consumer package, validated by `HadesTest` |
| `tests/testthat/helper-database-platforms.R` | Config, selection, environment, adapters, context | `HadesTest/R/` split by responsibility |
| `tests/testthat/helper.R::getPlatformConnectionDetails()` | Final orchestration wrapper | Replace with one exported context constructor |
| `tests/testthat/test-database-platforms.R` | Contract tests | Port generic cases to `HadesTest`; retain consumer integration cases |
| `tests/testthat/test-dbms-platforms.R` | Database-neutral package behavior | Remains in CohortGenerator |
| `tests/testthat/setup.R` | Avoid unnecessary SQLite fixture in live mode | Simplify with a shared mode predicate |
| `skipIfLiveDatabaseTest()` calls | Focus the live test process | Shared helper initially; revisit file-level opt-out later |
| `.github/workflows/database-platform-tests.yml` | One DBMS per isolated job | Thin consumer workflow plus reusable workflow/template |
| `.github/workflows/R_CMD_check_Hades.yaml` | Secret-free ordinary checks | Remains in every consumer package |

## Important Gaps to Fix During Extraction

These are properties of the current branch, not desired package behavior.

### 1. The declaration is not yet authoritative

Runtime connection construction does not use most YAML fields. `enabled`, `required`, `ci`, `connectionMode`, `credentialMode`, `environmentPrefix`, `cdmVersion`, and `tempEmulationSchema` are largely descriptive. The new package must make the declaration the source of truth or reject fields it does not implement.

### 2. Validation happens too late

`getPlatformConnectionDetails()` constructs a context before validating the required environment. Connection constructors can therefore receive incomplete values before the intended skip or failure is raised. The new order must be:

```text
read config -> select platform -> validate platform -> resolve environment
-> validate environment -> construct credentials/connection -> return context
```

### 3. `required` is not honored

In the prototype, any missing environment variable fails when `HADES_DATABASE_TEST=true`, regardless of the platform's `required` value. Strictness must combine explicit CI intent with the selected platform's policy.

### 4. CI tiers and triggers disagree

The YAML assigns `pull-request` to some platforms and `scheduled` to others, but the workflow runs all eight external platforms on matching pushes and pull requests and has no scheduled trigger. CI jobs must be derived from, or checked against, the declared tier.

### 5. BigQuery cannot run in the current job shape

The helper marks BigQuery as Windows-only, while every live matrix job uses Ubuntu. The current test returns `NULL` and can finish without opening a BigQuery connection. The shared tooling must fail a required job when a selected platform is incompatible with the runner, and the workflow must choose a compatible runner.

### 6. Configuration validation is shallow

The current loader checks only schema version, package name, and the top-level database relationship. It does not fully validate required fields, unique DBMS entries, field types, allowed CI tiers, disabled reasons, or relationships between fields.

### 7. Source-tree discovery depends on the working directory

The development fallback builds `inst/test-config/hades-database-platforms.yml` from `getwd()`. Direct `testthat::test_dir()` execution changes the working directory to `tests/testthat`, so configuration lookup can fail unless the package is installed or loaded through package-aware tooling. The shared locator must search from an explicit package root or use a robust package-development mechanism.

### 8. Matrix drift is possible

The platform list is duplicated in YAML and GitHub Actions. No check currently proves they agree.

### 9. Temporary driver workarounds are mixed with policy

Spark appends `EnableArrow=0`; Snowflake appends `JDBC_QUERY_RESULT_FORMAT=JSON`. These were explicitly introduced as temporary workarounds. Put them behind named, tested platform adapters and establish removal criteria rather than making them invisible permanent behavior.

### 10. Global options are not scoped

`sqlRenderTempEmulationSchema` is mutated globally. The shared package should use `withr::local_options()` or an equivalent lifecycle so state is restored even when a test fails.

### 11. Environment names remain package-era conventions

The current implementation knows `CDM5_*` and `CDM_*` names directly. The new public contract should use normalized `HADES_*` variables, with explicit legacy aliases during migration.

## Recommended Package Design

### Package role

`HadesTest` should be a test-support package installed through `Suggests` by consumers. It may import `DatabaseConnector`, `yaml`, and `withr`. If its skip helpers directly call `testthat::skip()`, it should also import `testthat`; otherwise, keep the core condition-based and place testthat integration in a thin adapter.

A pragmatic first release can import `testthat` because every consumer is using the package only in tests. Avoid R6 until the context needs mutable lifecycle behavior.

### Proposed source layout

```text
R/
  config.R
  platform.R
  environment.R
  context.R
  object-names.R
  skip.R
  ci-matrix.R
  adapters-bigquery.R
  adapters-iris.R
  adapters-oracle.R
  adapters-postgresql.R
  adapters-redshift.R
  adapters-snowflake.R
  adapters-spark.R
  adapters-sql-server.R
  zzz.R
inst/
  schema/hades-database-platforms.schema.json
  templates/hades-database-platforms.yml
  templates/database-platform-tests.yml
.github/workflows/
  reusable-database-platform-test.yml
tests/testthat/
  fixtures/
```

JSON Schema is useful for editor and non-R validation, but R validation must remain the normative implementation for the first release.

### Proposed public API for version 0.1.0

```r
readDatabaseTestConfig(package = NULL, path = NULL)
validateDatabaseTestConfig(config, package = NULL)
getConfiguredDatabasePlatforms(config, ci = NULL, enabled = TRUE)
getSelectedTestDbms(config, value = Sys.getenv("HADES_TEST_DBMS"))
getDatabasePlatformConfig(config, dbms = getSelectedTestDbms(config))
isLiveDatabaseTest()
skipIfNoLiveDatabase(config)
skipIfLiveDatabaseTest()
resolveDatabaseTestEnvironment(platform, aliases = NULL)
validateDatabaseTestEnvironment(platform, environment, strict = NULL)
createDatabaseTestContext(config, dbms = NULL, legacyAliases = NULL)
withDatabaseTestContext(config, code, dbms = NULL, legacyAliases = NULL)
createUniqueDatabaseObjectName(prefix, maxLength = NULL)
getDatabaseTestMatrix(config, ci)
validateDatabaseTestWorkflow(config, workflow)
```

Keep lower-level adapter functions internal until a second consuming package demonstrates an extension requirement.

### Context contract

Use a validated S3 list with a stable print method:

```r
list(
  dbms = "postgresql",
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "...",
  vocabularyDatabaseSchema = "...",
  workDatabaseSchema = "...",
  cdmVersion = "5.4",
  tempEmulationSchema = NULL,
  objectPrefix = "cg_...",
  platform = platformConfig
)
```

Class it as `hades_database_test_context`. Never print credentials or a complete connection string. The package should not connect eagerly unless connection verification is explicitly requested; context construction and connection verification should be separately testable.

### Configuration direction

Keep the current high-level fields, but make their semantics executable. A consumer declaration should describe support and CI policy without containing secret values.

For the normalized interface, prefer these process-level variables:

```text
HADES_TEST_DBMS
HADES_DATABASE_TEST
HADES_DB_USER
HADES_DB_PASSWORD
HADES_DB_SERVER
HADES_DB_CONNECTION_STRING
HADES_DB_KEY_FILE
HADES_CDM_SCHEMA
HADES_VOCABULARY_SCHEMA
HADES_WORK_SCHEMA
HADES_CDM_VERSION
```

Existing GitHub secrets can be mapped to these names in each matrix job. During migration, consumer-supplied alias maps can fall back to the historical `CDM5_*` and `CDM_*` variables.

Do not infer every legacy variable solely from `environmentPrefix`; the existing names contain exceptions such as `SERVER_SERVER`, `CDM53_SCHEMA`, and BigQuery's distinct prefix.

## Implementation Plan

### Phase 0: Confirm the contract

Deliverable: a short accepted design note or issue.

- Confirm package and repository name.
- Confirm that the package targets live DBMS test infrastructure only.
- Decide whether `testthat` is an import or an optional adapter.
- Confirm normalized `HADES_*` environment names.
- Define the supported CI tiers: `pull-request`, `push`, `scheduled`, `manual`, and `none`.
- Decide whether `required` applies per selected CI tier or globally.
- Confirm BigQuery runner support.
- Select one second HADES package for the pilot migration.

Exit criterion: maintainers agree on the public context shape, environment contract, and configuration semantics.

### Phase 1: Scaffold the package and continuous checks

Deliverable: an installable package with no live-database dependency in its unit-test path.

- Create the package with an Apache 2.0 license and HADES-compatible metadata.
- Add `testthat` edition 3, `lintr`, `R CMD check`, coverage, and pkgdown.
- Add contribution and release guidance.
- Add a configuration fixture copied from CohortGenerator, with fake environment names only.
- Establish semantic versioning and lifecycle badges for experimental APIs.

Exit criterion: clean local and GitHub `R CMD check` without credentials.

### Phase 2: Implement the pure configuration layer

Deliverable: configuration can be located, parsed, normalized, and rejected without loading JDBC drivers.

- Implement the top-level `databaseConnection` modes.
- Validate all platform fields and cross-field rules.
- Reject duplicate canonical DBMS names.
- Require a reason for disabled platforms.
- Normalize aliases such as `sqlserver` to `sql server` only at input boundaries.
- Implement CI-tier filtering.
- Add a JSON Schema that matches the R validator.
- Test source-tree and installed-package discovery.

Exit criterion: malformed fixtures cover every validation rule and produce actionable, secret-free errors.

### Phase 3: Implement environment resolution and policy

Deliverable: credentials and schemas are resolved before any connection object is constructed.

- Implement normalized variables.
- Implement explicit, consumer-provided legacy aliases.
- Return a redacted environment object.
- Model missing values with typed conditions.
- Apply local skip, optional CI skip, and required CI failure consistently.
- Ensure condition messages name missing variables but never values.

Exit criterion: a truth-table test covers local/CI, required/optional, selected/unselected, and complete/incomplete combinations.

### Phase 4: Implement platform adapters and the context

Deliverable: each supported DBMS can produce the same context contract.

- Port one adapter first, preferably PostgreSQL.
- Separate connection-detail construction from live connection verification.
- Add the remaining server and connection-string adapters.
- Scope and restore SqlRender options.
- Securely create and delete temporary BigQuery key files.
- Make runner compatibility explicit and fail required incompatible jobs.
- Encapsulate Spark and Snowflake workarounds behind named adapter options.
- Add DBMS-aware object-name length and character rules.

Use mocks or injected constructors for unit tests; do not require live databases for package `R CMD check`.

Exit criterion: every adapter has unit tests for arguments, redaction, cleanup, and edge cases.

### Phase 5: Add testthat integration and lifecycle helpers

Deliverable: a consumer test can obtain a context with a small, consistent setup.

- Implement `skipIfNoLiveDatabase()` and `skipIfLiveDatabaseTest()`.
- Implement `withDatabaseTestContext()` with guaranteed local cleanup of package-owned temporary resources.
- Do not attempt to delete consumer-owned tables automatically.
- Provide a vignette showing a package behavioral test and `on.exit()` cleanup.
- Provide a migration guide from copied helper files.

Exit criterion: an example package test uses no platform-specific branch in its behavioral assertions.

### Phase 6: Add CI support

Deliverable: consumer CI and its declaration cannot silently drift.

- Implement `getDatabaseTestMatrix()`.
- Add a command or test helper that compares the static caller matrix with configuration.
- Add a reusable workflow that accepts DBMS, runner, GitHub environment, package path, and R version.
- Keep secret-to-normalized-variable mapping at the caller boundary.
- Make required jobs fail if no live connection was attempted.
- Add a scheduled trigger example and fork-PR guidance.
- Pin the reusable workflow by released tag in consumers.

Exit criterion: fixture workflows prove tier filtering, runner selection, missing-secret failure, and matrix/config agreement.

### Phase 7: Migrate CohortGenerator

Deliverable: CohortGenerator consumes a released or pinned development version of the shared package.

- Add `HadesTest` to `Suggests`.
- Replace generic functions in `helper-database-platforms.R` with package calls.
- Retain only CohortGenerator's behavioral test and cleanup.
- Map current secrets to normalized environment variables in the workflow.
- Correct the BigQuery runner.
- Align workflow triggers with declared CI tiers.
- Remove obsolete helper tests after equivalent shared-package tests exist.
- Keep a small integration test proving the installed declaration is found.

Exit criterion: the ordinary check is secret-free, each required live job proves a connection attempt, and no shared helper implementation remains copied in CohortGenerator.

### Phase 8: Pilot, stabilize, and release

Deliverable: version 0.1.0 validated by at least two materially different HADES packages.

- Migrate the selected second package.
- Record extension points discovered by the second migration.
- Avoid expanding the public API unless both consumers need it.
- Run required platforms and at least one scheduled platform in both packages.
- Publish a compatibility and migration table.
- Tag `0.1.0`, then update consumers to the tag.

Exit criterion: two packages share the implementation, maintain only package-specific behavior, and use the same configuration and CI contracts.

## Recommended Pull Request Sequence

Keep the extraction reviewable with the following PRs:

1. package skeleton, checks, and design note;
2. config schema, parser, validator, and fixtures;
3. environment resolver and typed strictness conditions;
4. PostgreSQL adapter and context contract;
5. remaining adapters and temporary-resource lifecycle;
6. testthat helpers, vignette, and migration guide;
7. CI matrix tools and reusable workflow;
8. CohortGenerator migration;
9. second-package pilot; and
10. version 0.1.0 release.

Do not begin with a mechanical copy of `helper-database-platforms.R`. Start with the contract and its tests, then port behavior adapter by adapter.

## First Implementation PR

The next concrete step should be PR 1 plus the pure part of PR 2:

1. create the new package repository;
2. add package metadata, license, checks, and testthat infrastructure;
3. add the proposed configuration fixture;
4. implement `readDatabaseTestConfig()` and `validateDatabaseTestConfig()`;
5. test every top-level and platform-level validation rule; and
6. document the context contract without implementing live connections yet.

This first PR is intentionally credential-free and can be reviewed on API and schema quality before JDBC behavior is introduced.

## Acceptance Criteria for Version 0.1.0

- A consumer has exactly one support/test declaration.
- All documented configuration fields affect behavior or are rejected.
- One process selects exactly one DBMS.
- Missing local credentials skip with a useful message.
- Missing credentials for a required CI job fail before connection construction.
- Optional CI behavior is explicit and tested.
- Every required green job proves that a connection was attempted.
- No error, print method, or log exposes secrets or full connection strings.
- Temporary credential files and global options are restored on success and failure.
- Platform adapters share one context contract.
- CI tier and runner choice are validated against the declaration.
- Matrix/declaration drift is detected automatically.
- Ordinary package checks receive no live database credentials.
- CohortGenerator contains no copied generic adapter implementation.
- A second HADES package has completed the migration.

## Decisions to Record Before Coding Beyond Phase 1

| Decision | Recommended default | Why it matters |
|---|---|---|
| Package name | `HadesTest` as a working name | Avoids hard-coding a name before governance review |
| Test framework dependency | Import `testthat` for 0.1.0 | Simplifies a test-only package and consumer usage |
| Context type | Validated S3 list | Stable contract without unnecessary mutable state |
| Environment contract | Normalized `HADES_*` names | Removes DBMS-specific branching from consumers |
| Legacy support | Consumer-supplied aliases | Makes exceptions explicit and removable |
| Matrix strategy | Static caller matrix plus validation first | Easier to audit than dynamic workflow generation |
| Workflow reuse | Versioned reusable workflow | Centralizes setup while preserving package triggers |
| CDM provisioning | Out of scope | Keeps the first package focused and deliverable |

## Handoff Checklist

- [ ] Confirm the package/repository name.
- [ ] Open the design issue with the Phase 0 decisions.
- [ ] Choose the second pilot package.
- [ ] Create the package repository and its first project board or milestone.
- [ ] Implement the first PR described above.
- [ ] Keep CohortGenerator unchanged until the shared config contract is tested.
- [ ] Migrate PostgreSQL first, then add adapters individually.
- [ ] Resolve the BigQuery runner mismatch before calling the prototype complete.
- [ ] Remove Spark and Snowflake workarounds when upstream behavior permits.
- [ ] Release a tagged version before broad HADES adoption.
