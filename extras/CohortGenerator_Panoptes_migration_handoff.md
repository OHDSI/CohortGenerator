# CohortGenerator Panoptes Migration Handoff

> **Panoptes 0.1.0 update:** This handoff reflects the released API. Panoptes
> validates one complete workflow matrix containing every enabled non-local
> platform. Current decisions and verification results are in
> `extras/Panoptes_migration_implementation_notes.md`.

## Purpose and target outcome

This handoff describes how to refactor the CohortGenerator
`update-database-testing-approach` branch to consume Panoptes instead of owning
a copied implementation of multi-database test infrastructure.

The completed migration should leave CohortGenerator responsible for its
database declaration, cohort-generation behavior test, fixtures, assertions,
and cleanup. Panoptes should own configuration validation, DBMS selection,
environment resolution, connection-detail adapters, temporary credentials,
scoped SqlRender options, object-name generation, connection-attempt policy,
and workflow-matrix validation.

Snapshot used for this handoff:

- CohortGenerator repository: `C:/git/ohdsi/CohortGenerator`;
- CohortGenerator branch: `update-database-testing-approach`;
- CohortGenerator commit: `73fb77f`;
- Panoptes repository: `C:/git/anthonysena/Panoptes`;
- Panoptes version: `0.1.0`;
- Panoptes commit at inspection: `6aab3dc`; and
- handoff date: 2026-10-22.

Do not merge this migration until the Panoptes dependency source is stable and
at least the PostgreSQL live job has passed through the new path.

## Migration boundary

### Move to Panoptes

Remove the CohortGenerator copies of:

- YAML discovery and validation;
- selected-DBMS normalization;
- required environment-variable definitions;
- missing-environment skip/failure policy;
- JDBC connection-detail constructors;
- Spark and Snowflake connection-string normalization;
- BigQuery key-file handling;
- runner and operating-system compatibility checks;
- unique cohort-table suffix generation;
- SqlRender option lifecycle;
- connection-attempt verification; and
- CI matrix/declaration comparison.

### Keep in CohortGenerator

Retain:

- `inst/test-config/hades-database-platforms.yml` as CohortGenerator's support
  declaration;
- the behavior currently exercised by `test-dbms-platforms.R`;
- cohort-definition, subset, and negative-control fixtures;
- assertions over generation and count results;
- cleanup through `dropCohortStatsTables()` and `unlink()`;
- selection of CI triggers and protected GitHub environments; and
- a small integration test proving that Panoptes can locate the installed
  CohortGenerator declaration.

### Keep out of both packages

Do not add CDM provisioning, database-server creation, secret values, secret
rotation, or automatic deletion of consumer-owned database objects to this
migration.

## Current and target execution flows

The prototype currently executes in this order:

```text
read shallow YAML
  -> select DBMS
  -> construct connection details and temporary credentials
  -> validate environment
  -> download JDBC driver
  -> mutate global SqlRender option
  -> run CohortGenerator behavior
```

The Panoptes path should execute in this order:

```text
read and fully validate declaration
  -> select and validate one DBMS
  -> enforce enabled, required, runner, and OS policy
  -> resolve normalized environment and optional legacy aliases
  -> validate required values
  -> create temporary credentials and connection details
  -> verify a live connection
  -> scope SqlRender options
  -> run CohortGenerator behavior and cleanup
```

The ordering change is important: incomplete or incompatible configuration
must fail or skip before JDBC objects, key files, or package behavior are
created.

## Decisions required before implementation

Resolve these items in the migration pull request description:

1. **Panoptes source.** Prefer a released tag such as
   `OHDSI/Panoptes@v0.1.0`. Until governance and transfer are complete, pin an
   exact commit from `anthonysena/Panoptes`; do not depend on a moving branch.
2. **GitHub secrets.** Prefer the same normalized secret names in every
   DBMS-specific GitHub environment. Use legacy aliases only as a temporary
   local-development bridge.
3. **BigQuery runner.** This handoff preserves the Panoptes 0.1.0 declaration
   of `windows-latest` and `os: windows`. Change it only after the current
   DatabaseConnector and BigQuery driver path is verified elsewhere.
4. **Fork pull requests.** Protected credentials are unavailable to untrusted
   forks. Keep live jobs disabled for fork code unless a maintainer initiates a
   trusted run.

## File change inventory

| CohortGenerator file | Required action |
|---|---|
| `DESCRIPTION` | Add Panoptes to `Suggests`; pin its development source until released; remove `yaml` if unused after migration; keep `withr` because other tests use it |
| `inst/test-config/hades-database-platforms.yml` | Replace prototype fields with the Panoptes schema shown below |
| `tests/testthat/helper-database-platforms.R` | Delete after the Panoptes behavior path passes |
| `tests/testthat/helper.R` | Remove only `getPlatformConnectionDetails()`; retain CohortGenerator fixture helpers |
| `tests/testthat/test-database-platforms.R` | Delete generic infrastructure tests; replace with the small consumer integration test below |
| `tests/testthat/test-dbms-platforms.R` | Change `testPlatform()` to accept a Panoptes context and wrap the test in `withDatabaseTestContext()` |
| `tests/testthat/setup.R` | Keep the selected-DBMS guard that avoids unnecessary Eunomia setup; do not use `isLiveDatabaseTest()` as a replacement because local selected-DBMS runs may set `HADES_DATABASE_TEST=false` |
| Ordinary `tests/testthat/test-*.R` files | Replace unqualified `skipIfLiveDatabaseTest()` calls with `Panoptes::skipIfLiveDatabaseTest()` |
| `.github/workflows/database-platform-tests.yml` | Include every enabled external platform with normalized variables and each row's declared runner |
| `.github/workflows/R_CMD_check_Hades.yaml` | Keep secret-free; no live database variables should be added back |
| `NEWS.md` | Record adoption of Panoptes and removal of copied adapters |

## Add the Panoptes dependency

Add Panoptes to `Suggests` because it is test infrastructure:

```text
Suggests:
  ...,
  Panoptes (>= 0.1.0),
  ...
```

Until a released source is available, use an exact development pin. For
example:

```text
Remotes:
  anthonysena/Panoptes@6aab3dc
```

Replace that provisional remote with the approved OHDSI repository and release
tag before broad adoption. `yaml` can be removed from CohortGenerator's
`Suggests` after `helper-database-platforms.R` and
`test-database-platforms.R` are deleted; no other source file currently uses
it. Keep `withr`, which is used by several remaining tests.

## Replace the database declaration

Replace `inst/test-config/hades-database-platforms.yml` with the declaration
below. Panoptes rejects `environmentPrefix` because it cannot model the legacy
naming exceptions. The executable `runner`, `os`, and `githubEnvironment`
fields replace hard-coded workflow policy.

```yaml
schemaVersion: 1
package: CohortGenerator
databaseConnection: all
platforms:
  - dbms: sqlite
    enabled: true
    required: false
    cdmVersion: "5.4"
    connectionMode: local
    credentialMode: none
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-sqlite

  - dbms: duckdb
    enabled: true
    required: false
    cdmVersion: "5.4"
    connectionMode: local
    credentialMode: none
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-duckdb

  - dbms: postgresql
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: server
    credentialMode: password
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-postgresql

  - dbms: sql server
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: server
    credentialMode: password
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-sql-server

  - dbms: oracle
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: server
    credentialMode: password
    tempEmulationSchema: true
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-oracle

  - dbms: redshift
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: server
    credentialMode: password
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-redshift

  - dbms: spark
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: connectionString
    credentialMode: password
    tempEmulationSchema: true
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-spark

  - dbms: bigquery
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: connectionString
    credentialMode: keyFile
    tempEmulationSchema: true
    runner: windows-latest
    os: windows
    githubEnvironment: test-bigquery

  - dbms: snowflake
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: connectionString
    credentialMode: password
    tempEmulationSchema: true
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-snowflake

  - dbms: iris
    enabled: true
    required: true
    cdmVersion: "5.4"
    connectionMode: connectionString
    credentialMode: password
    tempEmulationSchema: false
    runner: ubuntu-latest
    os: any
    githubEnvironment: test-iris
```

Validate the file before changing tests:

```r
config <- Panoptes::readDatabaseTestConfig(
  path = "inst/test-config/hades-database-platforms.yml"
)
stopifnot(config$package == "CohortGenerator")
stopifnot(length(config$platforms) == 10L)
```

## Normalize environment variables

Panoptes consumes these names:

| Variable | Use |
|---|---|
| `HADES_TEST_DBMS` | One canonical DBMS selected for the R process |
| `HADES_DATABASE_TEST` | Explicit live CI intent; use `true` in live jobs |
| `HADES_DB_USER` | User for password-based adapters |
| `HADES_DB_PASSWORD` | Password for password-based adapters |
| `HADES_DB_SERVER` | Server for `server` mode |
| `HADES_DB_CONNECTION_STRING` | JDBC string for `connectionString` mode |
| `HADES_DB_KEY_FILE` | Existing key path or key contents for BigQuery |
| `HADES_CDM_SCHEMA` | CDM schema |
| `HADES_VOCABULARY_SCHEMA` | Optional vocabulary schema; defaults to CDM |
| `HADES_WORK_SCHEMA` | Writable cohort and temporary-emulation schema |
| `HADES_CDM_VERSION` | Optional override; otherwise declaration value |

The recommended CI design is to create the same normalized secret names in
each protected GitHub environment. Because each matrix row selects one GitHub
environment, a PostgreSQL job receives only PostgreSQL values even though the
workflow uses common names.

Legacy-to-normalized mappings are:

| DBMS | Normalized variable | Current secret or environment variable |
|---|---|---|
| PostgreSQL | user, password, server | `CDM5_POSTGRESQL_USER`, `CDM5_POSTGRESQL_PASSWORD`, `CDM5_POSTGRESQL_SERVER` |
| PostgreSQL | CDM, work | `CDM5_POSTGRESQL_CDM_SCHEMA`, `CDM5_POSTGRESQL_OHDSI_SCHEMA` |
| SQL Server | user, password, server | `CDM5_SQL_SERVER_USER`, `CDM5_SQL_SERVER_PASSWORD`, `CDM5_SQL_SERVER_SERVER` |
| SQL Server | CDM, work | `CDM5_SQL_SERVER_CDM_SCHEMA`, `CDM5_SQL_SERVER_OHDSI_SCHEMA` |
| Oracle | user, password, server | `CDM5_ORACLE_USER`, `CDM5_ORACLE_PASSWORD`, `CDM5_ORACLE_SERVER` |
| Oracle | CDM, work | `CDM5_ORACLE_CDM_SCHEMA`, `CDM5_ORACLE_OHDSI_SCHEMA` |
| Redshift | user, password, server | `CDM5_REDSHIFT_USER`, `CDM5_REDSHIFT_PASSWORD`, `CDM5_REDSHIFT_SERVER` |
| Redshift | CDM, work | `CDM5_REDSHIFT_CDM_SCHEMA`, `CDM5_REDSHIFT_OHDSI_SCHEMA` |
| Spark | user, password, connection string | `CDM5_SPARK_USER`, `CDM5_SPARK_PASSWORD`, `CDM5_SPARK_CONNECTION_STRING` |
| Spark | CDM, work | `CDM5_SPARK_CDM_SCHEMA`, `CDM5_SPARK_OHDSI_SCHEMA` |
| BigQuery | connection string, key | `CDM_BIG_QUERY_CONNECTION_STRING`, `CDM_BIG_QUERY_KEY_FILE` |
| BigQuery | CDM, work | `CDM_BIG_QUERY_CDM_SCHEMA`, `CDM_BIG_QUERY_OHDSI_SCHEMA` |
| Snowflake | user, password, connection string | `CDM_SNOWFLAKE_USER`, `CDM_SNOWFLAKE_PASSWORD`, `CDM_SNOWFLAKE_CONNECTION_STRING` |
| Snowflake | CDM, work | `CDM_SNOWFLAKE_CDM53_SCHEMA`, `CDM_SNOWFLAKE_OHDSI_SCHEMA` |
| IRIS | user, password, connection string | `CDM_IRIS_USER`, `CDM_IRIS_PASSWORD`, `CDM_IRIS_CONNECTION_STRING` |
| IRIS | CDM, work | `CDM_IRIS_CDM_SCHEMA`, `CDM_IRIS_OHDSI_SCHEMA` |

### Optional local legacy bridge

If local developers cannot immediately rename their environment variables,
retain one consumer-specific helper containing aliases only. Do not retain
connection constructors or validation policy.

```r
getCohortGeneratorLegacyAliases <- function(dbms) {
  switch(
    dbms,
    postgresql = list(
      HADES_DB_USER = "CDM5_POSTGRESQL_USER",
      HADES_DB_PASSWORD = "CDM5_POSTGRESQL_PASSWORD",
      HADES_DB_SERVER = "CDM5_POSTGRESQL_SERVER",
      HADES_CDM_SCHEMA = "CDM5_POSTGRESQL_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM5_POSTGRESQL_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM5_POSTGRESQL_OHDSI_SCHEMA"
    ),
    "sql server" = list(
      HADES_DB_USER = "CDM5_SQL_SERVER_USER",
      HADES_DB_PASSWORD = "CDM5_SQL_SERVER_PASSWORD",
      HADES_DB_SERVER = "CDM5_SQL_SERVER_SERVER",
      HADES_CDM_SCHEMA = "CDM5_SQL_SERVER_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM5_SQL_SERVER_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM5_SQL_SERVER_OHDSI_SCHEMA"
    ),
    oracle = list(
      HADES_DB_USER = "CDM5_ORACLE_USER",
      HADES_DB_PASSWORD = "CDM5_ORACLE_PASSWORD",
      HADES_DB_SERVER = "CDM5_ORACLE_SERVER",
      HADES_CDM_SCHEMA = "CDM5_ORACLE_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM5_ORACLE_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM5_ORACLE_OHDSI_SCHEMA"
    ),
    redshift = list(
      HADES_DB_USER = "CDM5_REDSHIFT_USER",
      HADES_DB_PASSWORD = "CDM5_REDSHIFT_PASSWORD",
      HADES_DB_SERVER = "CDM5_REDSHIFT_SERVER",
      HADES_CDM_SCHEMA = "CDM5_REDSHIFT_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM5_REDSHIFT_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM5_REDSHIFT_OHDSI_SCHEMA"
    ),
    spark = list(
      HADES_DB_USER = "CDM5_SPARK_USER",
      HADES_DB_PASSWORD = "CDM5_SPARK_PASSWORD",
      HADES_DB_CONNECTION_STRING = "CDM5_SPARK_CONNECTION_STRING",
      HADES_CDM_SCHEMA = "CDM5_SPARK_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM5_SPARK_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM5_SPARK_OHDSI_SCHEMA"
    ),
    bigquery = list(
      HADES_DB_CONNECTION_STRING = "CDM_BIG_QUERY_CONNECTION_STRING",
      HADES_DB_KEY_FILE = "CDM_BIG_QUERY_KEY_FILE",
      HADES_CDM_SCHEMA = "CDM_BIG_QUERY_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM_BIG_QUERY_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM_BIG_QUERY_OHDSI_SCHEMA"
    ),
    snowflake = list(
      HADES_DB_USER = "CDM_SNOWFLAKE_USER",
      HADES_DB_PASSWORD = "CDM_SNOWFLAKE_PASSWORD",
      HADES_DB_CONNECTION_STRING = "CDM_SNOWFLAKE_CONNECTION_STRING",
      HADES_CDM_SCHEMA = "CDM_SNOWFLAKE_CDM53_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM_SNOWFLAKE_CDM53_SCHEMA",
      HADES_WORK_SCHEMA = "CDM_SNOWFLAKE_OHDSI_SCHEMA"
    ),
    iris = list(
      HADES_DB_USER = "CDM_IRIS_USER",
      HADES_DB_PASSWORD = "CDM_IRIS_PASSWORD",
      HADES_DB_CONNECTION_STRING = "CDM_IRIS_CONNECTION_STRING",
      HADES_CDM_SCHEMA = "CDM_IRIS_CDM_SCHEMA",
      HADES_VOCABULARY_SCHEMA = "CDM_IRIS_CDM_SCHEMA",
      HADES_WORK_SCHEMA = "CDM_IRIS_OHDSI_SCHEMA"
    ),
    list()
  )
}
```

Place this in `tests/testthat/helper-panoptes.R` only if needed. Give it a
removal date or follow-up issue after all developer and CI environments use
normalized variables.

## Refactor the CohortGenerator behavior test

Change `testPlatform()` in `test-dbms-platforms.R` to accept the Panoptes
context directly. Use these field mappings:

| Prototype field | Panoptes context field |
|---|---|
| `dbmsDetails$dbmsPlatform` | `context$dbms` |
| `dbmsDetails$connectionDetails` | `context$connectionDetails` |
| `dbmsDetails$cohortDatabaseSchema` | `context$workDatabaseSchema` |
| `dbmsDetails$cohortTable` | `context$objectPrefix` |
| `dbmsDetails$cdmDatabaseSchema` | `context$cdmDatabaseSchema` |
| `dbmsDetails$vocabularyDatabaseSchema` | `context$vocabularyDatabaseSchema` |

Use `context$objectPrefix` directly as the cohort table name. Panoptes has
already bounded it for the selected DBMS; appending another suffix can exceed
Oracle's identifier limit.

The test orchestration should become:

```r
test_that("platform specific create cohorts with stats, Incremental, get results", {
  skip_on_cran()

  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")
  Panoptes::skipIfNoLiveDatabase(config)
  dbms <- Panoptes::getSelectedTestDbms(config)

  aliases <- getCohortGeneratorLegacyAliases(dbms) # Remove after migration.

  Panoptes::withDatabaseTestContext(
    config = config,
    dbms = dbms,
    legacyAliases = NULL,
    verify = TRUE,
    {
      message("Testing ", context$dbms)
      testPlatform(context)
      Panoptes::requireDatabaseTestConnectionAttempted(context)
    }
  )
})
```

When the optional local legacy bridge is still needed, replace
`legacyAliases = NULL` with
`legacyAliases = getCohortGeneratorLegacyAliases(dbms)`. CI should use
normalized variables directly.

Inside `testPlatform()`, the relevant setup and cleanup should look like:

```r
testPlatform <- function(context) {
  cohortTableNames <- getCohortTableNames(
    cohortTable = context$objectPrefix
  )
  platformOutputFolder <- file.path(outputFolder, context$dbms)

  on.exit({
    dropCohortStatsTables(
      connectionDetails = context$connectionDetails,
      cohortDatabaseSchema = context$workDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = TRUE
    )
    unlink(platformOutputFolder, recursive = TRUE)
  }, add = TRUE)

  # Keep the existing fixture construction and assertions.

  runCohortGeneration(
    connectionDetails = context$connectionDetails,
    cdmDatabaseSchema = context$cdmDatabaseSchema,
    cohortDatabaseSchema = context$workDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithSubsets,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE,
    outputFolder = platformOutputFolder,
    databaseId = context$dbms,
    incremental = FALSE
  )

  # Keep the existing CSV row-count assertions.
}
```

`withDatabaseTestContext()` scopes `sqlRenderTempEmulationSchema` and removes a
temporary BigQuery key file even when behavior fails. CohortGenerator must
continue to remove its own cohort and statistics tables.

`verify = TRUE` opens and closes a connection before behavior begins. This
marks the Panoptes context as attempted so a required CI path cannot finish
green after an unsupported-runner or early-return path.

## Update setup and ordinary test focus

Keep the current selected-DBMS guard in `tests/testthat/setup.R`:

```r
if (!nzchar(Sys.getenv("HADES_TEST_DBMS", unset = ""))) {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
} else {
  connectionDetails <- NULL
}
```

This deliberately checks selection rather than
`Panoptes::isLiveDatabaseTest()`. A local developer can select a live DBMS while
leaving `HADES_DATABASE_TEST=false`; that run still should not construct the
Eunomia fixture.

In every ordinary test file currently calling the copied helper, replace:

```r
skipIfLiveDatabaseTest()
```

with:

```r
Panoptes::skipIfLiveDatabaseTest()
```

Do not import Panoptes into CohortGenerator's namespace. Namespaced calls keep
the dependency test-only and explicit.

## Replace generic helper tests with consumer integration tests

Delete `tests/testthat/test-database-platforms.R`; its configuration,
environment, adapter, normalization, context, and condition cases are now
tested in Panoptes.

Add a small CohortGenerator integration test such as
`tests/testthat/test-panoptes-integration.R`:

```r
Panoptes::skipIfLiveDatabaseTest()

test_that("Panoptes finds the installed CohortGenerator declaration", {
  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")

  expect_equal(config$package, "CohortGenerator")
  expect_equal(config$databaseConnection, "all")
  expect_equal(
    vapply(config$platforms, `[[`, character(1), "dbms"),
    c(
      "sqlite", "duckdb", "postgresql", "sql server", "oracle",
      "redshift", "spark", "bigquery", "snowflake", "iris"
    )
  )
})

test_that("CohortGenerator declares all external database platforms", {
  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")
  platforms <- Panoptes::getConfiguredDatabasePlatforms(config)
  externalPlatforms <- Filter(
    function(platform) !identical(platform$connectionMode, "local"),
    platforms
  )

  expect_equal(
    vapply(externalPlatforms, `[[`, character(1), "dbms"),
    c(
      "postgresql", "sql server", "oracle", "redshift", "spark",
      "bigquery", "snowflake", "iris"
    )
  )
})
```

Workflow-file drift should be checked in the workflow itself because
`.github` is excluded by CohortGenerator's `.Rbuildignore` and is unavailable
during built-package tests.

## Normalize the live database workflow

Panoptes 0.1.0 validates one matrix containing every enabled external platform.
Keep `.github/workflows/database-platform-tests.yml` with:

```yaml
strategy:
  fail-fast: false
  matrix:
    include:
      - dbms: postgresql
        runner: ubuntu-latest
        environment: test-postgresql
      - dbms: sql server
        runner: ubuntu-latest
        environment: test-sql-server
      - dbms: oracle
        runner: ubuntu-latest
        environment: test-oracle
      - dbms: redshift
        runner: ubuntu-latest
        environment: test-redshift
      - dbms: spark
        runner: ubuntu-latest
        environment: test-spark
      - dbms: bigquery
        runner: windows-latest
        environment: test-bigquery
      - dbms: snowflake
        runner: ubuntu-latest
        environment: test-snowflake
      - dbms: iris
        runner: ubuntu-latest
        environment: test-iris

runs-on: ${{ matrix.runner }}
environment: ${{ matrix.environment }}
```

Trigger it on `pull_request` and optionally `workflow_dispatch`. Avoid treating
an untrusted fork pull request as authorized to receive protected secrets.

### Common normalized environment

The workflow should use:

```yaml
env:
  HADES_DATABASE_TEST: "true"
  HADES_TEST_DBMS: ${{ matrix.dbms }}
  HADES_DB_USER: ${{ secrets.HADES_DB_USER }}
  HADES_DB_PASSWORD: ${{ secrets.HADES_DB_PASSWORD }}
  HADES_DB_SERVER: ${{ secrets.HADES_DB_SERVER }}
  HADES_DB_CONNECTION_STRING: ${{ secrets.HADES_DB_CONNECTION_STRING }}
  HADES_DB_KEY_FILE: ${{ secrets.HADES_DB_KEY_FILE }}
  HADES_CDM_SCHEMA: ${{ secrets.HADES_CDM_SCHEMA }}
  HADES_VOCABULARY_SCHEMA: ${{ secrets.HADES_VOCABULARY_SCHEMA }}
  HADES_WORK_SCHEMA: ${{ secrets.HADES_WORK_SCHEMA }}
  DATABASECONNECTOR_JAR_FOLDER: ${{ runner.temp }}/jdbcDrivers
```

Unset variables are acceptable when the selected adapter does not require
them. Panoptes validates only the selected platform's connection mode.

### Provision JDBC drivers explicitly

Panoptes intentionally does not download drivers during context construction.
After dependencies are installed, add:

```yaml
- name: Download JDBC driver
  shell: Rscript {0}
  run: |
    driverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
    dir.create(driverFolder, recursive = TRUE, showWarnings = FALSE)
    DatabaseConnector::downloadJdbcDrivers(
      dbms = Sys.getenv("HADES_TEST_DBMS"),
      pathToDriver = driverFolder
    )
```

Install a compatible JDK on every selected runner. The current Linux-only
`apt-get default-jdk` step is not sufficient for the Windows BigQuery row; use
a cross-platform Java setup action or a runner image with the required JDK.

### Validate the workflow matrix

Add this step after dependencies are installed:

```yaml
- name: Validate declared database matrix
  shell: Rscript {0}
  run: |
    config <- Panoptes::readDatabaseTestConfig(
      path = "inst/test-config/hades-database-platforms.yml"
    )
    Panoptes::validateDatabaseTestWorkflow(
      config,
      ".github/workflows/database-platform-tests.yml"
    )
```

This catches missing platforms, extra platforms, wrong runners, and wrong
GitHub environment names before live behavior starts.

## Delete copied infrastructure only after the new path passes

After a Panoptes-driven PostgreSQL job succeeds:

1. delete `tests/testthat/helper-database-platforms.R`;
2. remove `getPlatformConnectionDetails()` from
   `tests/testthat/helper.R`;
3. delete `tests/testthat/test-database-platforms.R`;
4. remove `yaml` from `Suggests` if a repository-wide search confirms no use;
5. confirm that no test refers to deleted helper functions; and
6. retain only `helper-panoptes.R` if the temporary legacy alias bridge is
   still required.

Useful searches are:

```powershell
rg -n "getDatabaseTestConfig|getDatabasePlatformConfig|getPlatformConnectionDetails|getDatabaseTestContext|resolveDatabasePlatformSettings" tests
rg -n "CDM5_|CDM_BIG_QUERY|CDM_SNOWFLAKE|CDM_IRIS" tests .github
rg -n "yaml::" R tests
```

The first and third searches should return nothing. The second should return
only the temporary alias bridge, if retained.

## Recommended pull request sequence

### Pull request 1: dependency and declaration

- Add the pinned Panoptes dependency.
- update the YAML declaration;
- add the consumer integration test; and
- validate the declared external-platform list without changing live behavior.

Exit criterion: ordinary checks pass without credentials and Panoptes loads the
installed declaration.

### Pull request 2: behavior path

- Refactor `test-dbms-platforms.R` around the Panoptes context;
- add normalized CI variables or the temporary alias bridge;
- provision JDBC drivers explicitly; and
- run PostgreSQL through Panoptes.

Exit criterion: PostgreSQL verifies a connection, runs existing cohort
behavior, asserts the same results, and cleans its objects.

### Pull request 3: matrix and runner correction

- Include every enabled external platform in one workflow;
- move BigQuery to Windows;
- add matrix validation steps; and
- run the complete matrix.

Exit criterion: workflow rows exactly match the declaration, and required jobs
cannot pass without a connection attempt.

### Pull request 4: remove copied infrastructure

- Delete the copied helper and generic tests;
- remove obsolete dependencies and legacy workflow variables; and
- update CohortGenerator documentation and NEWS.

Exit criterion: repository searches find no copied generic implementation.

These can be combined after the first successful pilot, but keeping deletion
last preserves a straightforward rollback while live credentials and runners
are being validated.

## Verification plan

### Credential-free local verification

Run from the CohortGenerator root:

```powershell
Rscript -e "devtools::document()"
Rscript -e "devtools::test(reporter = 'summary')"
Rscript -e "lintr::lint_package()"
R CMD build . --no-manual
R CMD check CohortGenerator_*.tar.gz --no-manual
```

Expected results:

- the ordinary suite uses Eunomia;
- no live credentials are required;
- the Panoptes integration tests pass;
- generic Panoptes behavior is not duplicated in CohortGenerator; and
- the ordinary check workflow remains secret-free.

### Missing local environment

```powershell
$env:HADES_TEST_DBMS = "postgresql"
$env:HADES_DATABASE_TEST = "false"
Rscript -e "devtools::test(filter = 'dbms-platforms', reporter = 'summary')"
```

Expected result: a skip naming missing normalized variables without showing
values.

### Missing required CI environment

```powershell
$env:HADES_TEST_DBMS = "postgresql"
$env:HADES_DATABASE_TEST = "true"
Rscript -e "devtools::test(filter = 'dbms-platforms', reporter = 'summary')"
```

Expected result: failure before connection details are constructed.

### Complete live environment

Set the normalized variables for one platform and run the same filtered test.
Start with PostgreSQL, then SQL Server and Oracle, and finally the remaining
external platforms.

Expected result: Panoptes verifies a connection, CohortGenerator runs the
existing behavior assertions, and cleanup succeeds.

## Acceptance criteria

The migration is complete when all of the following are true:

- CohortGenerator declares Panoptes in `Suggests` from an immutable source.
- The installed CohortGenerator declaration passes Panoptes validation.
- The declaration is the only platform-support source of truth.
- One R process selects exactly one DBMS.
- Local missing credentials skip with normalized variable names only.
- Missing credentials for a required live CI job fail before connection
  construction.
- The workflow matrix matches all enabled external platforms.
- BigQuery uses a compatible declared runner.
- Every required green live job records a connection attempt.
- SqlRender options and BigQuery temporary credentials are restored on success
  and failure.
- The existing cohort-generation and row-count assertions are unchanged in
  substance.
- CohortGenerator still owns and removes its database objects.
- The ordinary R CMD check workflow has no live secrets.
- `helper-database-platforms.R`, `getPlatformConnectionDetails()`, and generic
  infrastructure tests have been removed.
- Repository searches find no copied Panoptes adapters or policy.

## Risks and follow-up questions

1. Confirm the final Panoptes organization, repository, release tag, and minimum
   version before merging.
2. Verify BigQuery with the current Windows runner and DatabaseConnector
   version rather than assuming the prototype restriction remains necessary.
3. Establish removal criteria for the Spark `EnableArrow=0` and Snowflake
   `JDBC_QUERY_RESULT_FORMAT=JSON` workarounds, which Panoptes currently owns.
4. Decide when the optional legacy alias helper and old GitHub secret names can
   be deleted.
5. Confirm the environment-approval policy for live jobs.
6. Confirm the trusted-run policy for pull requests from forks.

## Rollback point

Do not delete the copied helper implementation until the Panoptes-driven
PostgreSQL job passes. If the pilot exposes an adapter or contract gap, keep the
CohortGenerator behavior test unchanged, revert only the Panoptes orchestration
commit, fix and release Panoptes, and then resume the migration. Do not patch a
second generic adapter implementation back into CohortGenerator.
