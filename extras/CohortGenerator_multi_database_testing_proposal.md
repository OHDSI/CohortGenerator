# CohortGenerator Multi-Database Testing Proposal

## Purpose

This document proposes a focused approach for standardizing live, multi-database testing across HADES packages, using OHDSI `CohortGenerator` as the first implementation and sandbox.

The design assumes:

- `DatabaseConnector` is the only database access layer used by HADES packages.
- Tests target live database servers.
- Each test server already contains an OMOP Common Data Model instance.
- CDM provisioning is outside the scope of the package test run.
- The same database-specific tests should run in GitHub Actions and locally when the required environment variables are available.
- CohortGenerator will serve as the initial implementation and proving ground before the approach is generalized across HADES.
- The design should remain declarative: packages describe which DBMS platforms they test, while shared tooling and CI decide how to run them.

---

## Current CohortGenerator Testing Architecture

CohortGenerator already contains most of the required building blocks, but the concerns are spread across several files.

### Package dependencies

CohortGenerator depends directly on `DatabaseConnector`. `Eunomia`, `testthat`, and `withr` are currently listed as suggested dependencies and are used by the test infrastructure.

### Platform declaration

The enabled external platforms are currently hard-coded in:

```text
tests/testthat/setup.R
```

The current declaration is approximately:

```r
dbmsPlatforms <- c(
  "bigquery",
  "oracle",
  "postgresql",
  "redshift",
  "spark",
  "sql server"
)
```

Snowflake is disabled through an inline comment.

### Connection configuration

The function `getPlatformConnectionDetails()` in:

```text
tests/testthat/helper.R
```

currently handles:

- JDBC driver setup;
- environment-variable lookup;
- `DatabaseConnector::createConnectionDetails()`;
- CDM schema selection;
- vocabulary schema selection;
- writable OHDSI/cohort schema selection;
- BigQuery key-file handling;
- SqlRender temporary-table emulation;
- randomized cohort table names.

The function uses a large conditional branch for each DBMS.

### Test execution

The live database test in:

```text
tests/testthat/test-dbms-platforms.R
```

loops through all declared platforms inside one `testthat` test.

Conceptually:

```r
for (dbmsPlatform in dbmsPlatforms) {
  dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)

  if (is.null(dbmsDetails)) {
    print(paste("No platform details available for", dbmsPlatform))
  } else {
    testPlatform(dbmsDetails)
  }
}
```

### GitHub Actions

The current `R CMD check` workflow uses a matrix across operating systems:

- Windows
- macOS
- Ubuntu

All database credentials are exposed as environment variables to each operating-system job. Database platforms are not represented as independent matrix jobs.

### Limitations

The current implementation works, but it creates several challenges:

- A DBMS failure is nested inside a larger operating-system job.
- GitHub check names do not identify which DBMS failed.
- Every job receives credentials for every platform.
- Platform declarations, test helpers, environment variables, and workflows can drift.
- Missing credentials can lead to a message instead of an explicit skip or failure.
- Running only one database platform locally is awkward.
- Live database testing may be repeated across operating systems without providing meaningful additional coverage.
- Disabled or partially supported platforms are represented informally in code comments.
- There is no machine-readable package-level support declaration.

---

# Proposed Architecture

The recommended architecture separates four concerns:

```text
Package declaration
        ↓
Runtime platform selection
        ↓
Database test context
        ↓
Shared behavioral tests
```

Each test process should target exactly one live database platform.

For the first implementation, that platform should be PostgreSQL.

---

## 1. Add a Package-Level Database Test Declaration

Create:

```text
inst/test-config/hades-database-platforms.yml
```

Placing the declaration under `inst/` makes it available from both the source repository and the installed package. It also allows the configuration to function as a machine-readable package support claim rather than only an internal test setting.

The declaration should answer one higher-level question first: does the package connect to databases at all?

- `databaseConnection: none` for packages that never connect to a database.
- `databaseConnection: subset` for packages that intentionally support only a subset of DatabaseConnector platforms.
- `databaseConnection: all` for packages that intend to support the full DatabaseConnector platform set.

This gives HADES a consistent way to distinguish packages like Andromeda, database-heavy packages like CohortGenerator, and packages that are database-free by design.

Add `yaml` to `Suggests` in `DESCRIPTION`.

### Proposed initial YAML

The first prototype should start with PostgreSQL only. Additional platforms can be added after the PostgreSQL flow is stable and the shape of the configuration is proven.

```yaml
schemaVersion: 1

package: CohortGenerator

databaseConnection: subset

platforms:
  - dbms: postgresql
    enabled: true
    required: true
    ci: pull-request
    environmentPrefix: CDM5_POSTGRESQL
    cdmVersion: "5.4"
    connectionMode: server
    tempEmulationSchema: false
```

The `ci` value above is illustrative and should be adjusted once the desired pull-request and scheduled-test policy is decided.

### Field definitions

#### `schemaVersion`

Version of the declaration format.

This allows the structure to evolve while shared tooling continues to validate older declarations.

#### `package`

Package owning the declaration.

#### `databaseConnection`

High-level database relationship for the package.

This field should use one of:

```text
none
subset
all
```

Packages that never connect to a database should use `none`. Packages that intentionally support only a subset of DatabaseConnector platforms should use `subset`. Packages that aim to support the full DatabaseConnector platform set should use `all`.

#### `dbms`

Canonical `DatabaseConnector` DBMS identifier.

The exact permitted values should eventually come from a shared HADES validator.

#### `enabled`

Whether the package currently claims support for and intends to test the platform.

In `subset` and `all` mode, `enabled: true` means the platform is part of the intended support or test surface. In `none` mode, platform entries should not normally be present.

#### `required`

Whether a selected CI job must run successfully.

For a required platform, missing credentials in CI should fail configuration rather than silently skip.

#### `ci`

Proposed automation tier. Candidate values:

```text
pull-request
main
scheduled
manual
none
```

This separates package support from CI frequency.

#### `environmentPrefix`

Prefix used to derive the existing platform-specific environment-variable names.

#### `cdmVersion`

Expected OMOP CDM version on the test server.

This is documentation and a future validation contract. It does not provision the database.

#### `connectionMode`

How `DatabaseConnector::createConnectionDetails()` should be populated.

Initial values:

```text
server
connectionString
```

#### `credentialMode`

Optional special credential handling, such as the BigQuery key file.

#### `tempEmulationSchema`

Whether the writable OHDSI schema should also be assigned to:

```r
options(sqlRenderTempEmulationSchema = ...)
```

#### `reason`

Required when a previously supported or recognized platform is disabled.

If `databaseConnection: none`, the package-level declaration should explain that the package is intentionally database-free rather than merely missing DBMS support.

### Data that should not appear in YAML

The YAML should not contain:

- server names;
- usernames;
- passwords;
- connection strings;
- key-file contents;
- actual schema names.

Those values remain in local environment variables or GitHub secrets.

For `databaseConnection: none`, no live database credentials or schema settings should be required by the package's test declaration.

---

## 2. Select One DBMS Per Test Process

Introduce a standard environment variable:

```text
HADES_TEST_DBMS
```

Examples:

```text
HADES_TEST_DBMS=postgresql
HADES_TEST_DBMS=oracle
HADES_TEST_DBMS=sql server
```

A GitHub Actions matrix sets this variable for each job. A developer sets it locally to select one platform.

The live test file should no longer loop over every DBMS.

### Current pattern

```r
for (dbmsPlatform in dbmsPlatforms) {
  testPlatform(getPlatformConnectionDetails(dbmsPlatform))
}
```

### Proposed pattern

```r
dbms <- Sys.getenv("HADES_TEST_DBMS", unset = "")

test_that("cohort generation works on the selected live DBMS", {
  skip_on_cran()
  skipIfNoLiveDatabase()

  context <- getDatabaseTestContext(dbms)
  testCohortGenerationPlatform(context)
})
```

### Benefits

- One DBMS per GitHub job.
- One DBMS per local test execution.
- Clear GitHub check names.
- Isolated failures.
- Easier retry and troubleshooting.
- Reduced credential exposure.
- No need to configure every platform locally.
- A database job cannot appear successful merely because another platform passed.

---

## 3. Introduce a Database Test Context

Replace the current large `getPlatformConnectionDetails()` function with smaller responsibilities:

1. Parse and validate the YAML.
2. Resolve the selected platform.
3. Validate required environment variables.
4. Construct `DatabaseConnector` connection details.
5. Return a standard test context.

### Proposed context

```r
list(
  dbms = "postgresql",
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "...",
  vocabularyDatabaseSchema = "...",
  cohortDatabaseSchema = "...",
  cohortTable = "...",
  cdmVersion = "5.4"
)
```

The first implementation can remain a named list. A formal S3 or R6 class is unnecessary until there is a demonstrated need.

### Proposed helper surface

```r
getDatabaseTestConfig()
getConfiguredDatabasePlatforms()
getSelectedTestDbms()
getDatabasePlatformConfig()
validateDatabaseTestEnvironment()
getDatabaseTestContext()
skipIfNoLiveDatabase()
isCiDatabaseTest()
```

These helpers can initially remain in:

```text
tests/testthat/helper-database-platforms.R
```

The file should be named around its responsibility rather than continuing to accumulate unrelated helpers in `helper.R`.

---

## 4. Configuration Loading

Example:

```r
getDatabaseTestConfig <- function() {
  configFile <- system.file(
    "test-config",
    "hades-database-platforms.yml",
    package = "CohortGenerator",
    mustWork = TRUE
  )

  config <- yaml::read_yaml(configFile)
  validateDatabaseTestConfig(config)
  config
}
```

When tests are run directly from the source tree, `system.file()` should work after `devtools::load_all()` or package installation. If needed, a development fallback can read:

```text
inst/test-config/hades-database-platforms.yml
```

directly.

### Validation expectations

The validator should check:

- `schemaVersion` is supported;
- `package` equals `CohortGenerator`;
- `platforms` is present;
- each `dbms` is unique;
- every platform has `enabled`, `required`, and `environmentPrefix`;
- disabled platforms include a `reason`;
- `connectionMode` is recognized;
- `ci` is one of the allowed values.

---

## 5. Platform Selection

```r
getSelectedTestDbms <- function() {
  dbms <- Sys.getenv("HADES_TEST_DBMS", unset = "")
  trimws(tolower(dbms))
}
```

The selected DBMS should be validated against the YAML declaration.

```r
getDatabasePlatformConfig <- function(
    dbms = getSelectedTestDbms(),
    config = getDatabaseTestConfig()) {
  if (!nzchar(dbms)) {
    return(NULL)
  }

  matches <- vapply(
    config$platforms,
    function(platform) identical(tolower(platform$dbms), dbms),
    logical(1)
  )

  if (!any(matches)) {
    stop(
      sprintf(
        "Database platform '%s' is not declared by CohortGenerator.",
        dbms
      ),
      call. = FALSE
    )
  }

  config$platforms[[which(matches)]]
}
```

---

## 6. Distinguish Local Skips From CI Failures

Missing credentials should behave differently depending on execution context.

| Context | Expected behavior |
|---|---|
| Normal package test with no live DBMS selected | Skip live database tests |
| Explicit local live DBMS test with incomplete variables | Skip with detailed explanation |
| GitHub matrix job for a required DBMS | Fail configuration |
| GitHub matrix job for an optional DBMS | Skip or allow failure according to policy |

### CI indicator

Use:

```text
HADES_DATABASE_TEST=true
```

in the database-specific GitHub workflow.

GitHub also sets `CI=true`, but a HADES-specific variable makes intent explicit and avoids treating unrelated CI execution as a required live database run.

```r
isCiDatabaseTest <- function() {
  identical(
    tolower(Sys.getenv("HADES_DATABASE_TEST", unset = "false")),
    "true"
  )
}
```

### Missing-variable behavior

```r
validateDatabaseTestEnvironment <- function(platform, strict = isCiDatabaseTest()) {
  requiredVariables <- getRequiredDatabaseEnvironmentVariables(platform)

  missingVariables <- requiredVariables[
    !nzchar(Sys.getenv(requiredVariables, unset = ""))
  ]

  if (length(missingVariables) == 0) {
    return(invisible(TRUE))
  }

  message <- paste0(
    "Missing environment variables for ",
    platform$dbms,
    ": ",
    paste(missingVariables, collapse = ", ")
  )

  if (strict && isTRUE(platform$required)) {
    stop(message, call. = FALSE)
  }

  testthat::skip(message)
}
```

This prevents a required CI job from turning green when no database test actually ran.

---

## 7. Environment Variable Conventions

There are two viable options.

### Option A: Preserve existing HADES variables

Examples:

```text
CDM5_POSTGRESQL_USER
CDM5_POSTGRESQL_PASSWORD
CDM5_POSTGRESQL_SERVER
CDM5_POSTGRESQL_CDM_SCHEMA
CDM5_POSTGRESQL_OHDSI_SCHEMA
```

Advantages:

- minimal migration;
- existing GitHub secrets continue to work;
- maintainers already understand the convention.

Disadvantages:

- names differ across platforms;
- some platforms use `CDM5_`, others use `CDM_`;
- CDM version is embedded inconsistently;
- shared helpers need more platform-specific mapping.

### Option B: Introduce normalized HADES test variables

Examples:

```text
HADES_DB_USER
HADES_DB_PASSWORD
HADES_DB_SERVER
HADES_DB_CONNECTION_STRING
HADES_CDM_SCHEMA
HADES_VOCABULARY_SCHEMA
HADES_WORK_SCHEMA
HADES_DB_KEY_FILE
```

Advantages:

- every matrix job presents the same interface to R;
- shared helpers are simpler;
- GitHub environments can isolate secrets by DBMS;
- easier reuse across HADES packages.

Disadvantages:

- requires secret migration or mapping;
- temporarily duplicates conventions.

### Recommended transition

Support both:

1. normalized `HADES_*` variables as the preferred interface;
2. existing platform-specific variables as fallback aliases.

For example:

```r
getEnvValue <- function(primary, fallback = NULL) {
  value <- Sys.getenv(primary, unset = "")

  if (nzchar(value) || is.null(fallback)) {
    return(value)
  }

  Sys.getenv(fallback, unset = "")
}
```

For PostgreSQL:

```r
user <- getEnvValue(
  "HADES_DB_USER",
  "CDM5_POSTGRESQL_USER"
)
```

This permits gradual adoption without requiring an immediate organization-wide secret migration.

---

## 8. Constructing the DatabaseConnector Context

The YAML should describe behavior, while R code remains responsible for creating the `DatabaseConnector` objects.

Example outline:

```r
getDatabaseTestContext <- function(
    dbms = getSelectedTestDbms(),
    strict = isCiDatabaseTest()) {
  config <- getDatabaseTestConfig()
  platform <- getDatabasePlatformConfig(dbms, config)

  if (is.null(platform)) {
    testthat::skip(
      "Set HADES_TEST_DBMS to run live database platform tests."
    )
  }

  if (!isTRUE(platform$enabled)) {
    testthat::skip(
      sprintf(
        "%s testing is disabled: %s",
        platform$dbms,
        platform$reason
      )
    )
  }

  validateDatabaseTestEnvironment(platform, strict = strict)

  jdbcDriverFolder <- getJdbcDriverFolder()
  DatabaseConnector::downloadJdbcDrivers(
    platform$dbms,
    pathToDriver = jdbcDriverFolder
  )

  connectionDetails <- createPlatformConnectionDetails(
    platform = platform,
    jdbcDriverFolder = jdbcDriverFolder
  )

  cdmSchema <- getPlatformCdmSchema(platform)
  vocabularySchema <- getPlatformVocabularySchema(platform)
  workSchema <- getPlatformWorkSchema(platform)

  configureSqlRenderTestOptions(
    platform = platform,
    workSchema = workSchema
  )

  list(
    dbms = platform$dbms,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmSchema,
    vocabularyDatabaseSchema = vocabularySchema,
    cohortDatabaseSchema = workSchema,
    cohortTable = createUniqueTestTableName("ct"),
    cdmVersion = platform$cdmVersion
  )
}
```

### JDBC driver folder

Retain current behavior:

1. use `DATABASECONNECTOR_JAR_FOLDER` when configured;
2. otherwise use `~/.jdbcDrivers`;
3. create the folder when needed.

### Unique table names

Continue generating collision-resistant table names.

A more readable helper would be:

```r
createUniqueTestTableName <- function(prefix) {
  paste0(
    prefix,
    "_",
    Sys.getpid(),
    "_",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "_",
    sample.int(1000000, 1)
  )
}
```

This is still not a substitute for a fully isolated writable schema, but it reduces collisions when multiple jobs share the same schema.

---

## 9. Preserve Database-Neutral Behavioral Tests

The existing `testPlatform()` behavior is a useful initial cross-platform smoke test. It currently:

- creates cohort table names;
- loads cohort definitions;
- builds cohort subset operations;
- adds negative-control cohorts;
- calls `runCohortGeneration()`;
- reads output CSV files;
- verifies generated cohort counts;
- cleans up database tables and output files.

This should be preserved and renamed to emphasize that it is shared behavior:

```r
testCohortGenerationPlatform <- function(context) {
  ...
}
```

The test body should use only the standard context fields and should not contain platform-specific environment-variable logic.

### Proposed test file

```r
test_that("cohort generation works on the selected live database", {
  skip_on_cran()
  skipIfNoLiveDatabase()

  context <- getDatabaseTestContext()

  testCohortGenerationPlatform(context)
})
```

### Explicit connection verification

The test should verify that a connection was actually established before running package behavior.

```r
connection <- DatabaseConnector::connect(
  connectionDetails = context$connectionDetails
)

withr::defer(
  DatabaseConnector::disconnect(connection),
  testthat::teardown_env()
)

expect_true(DatabaseConnector::dbIsValid(connection))
```

Use the appropriate DatabaseConnector-supported validation mechanism available in the package version. If no direct validation helper exists, execute a minimal query through DatabaseConnector.

The important contract is that a selected CI platform must prove that a live connection was opened.

---

## 10. Cleanup and Writable Schema Isolation

The live tests create cohort and statistics tables in the configured writable schema.

The current randomized cohort table name helps avoid collisions, but related tables may still share generated names derived from the cohort table.

The initial standard should require:

- a writable schema;
- permission to create and drop test tables;
- randomized table prefixes;
- cleanup through `on.exit()` or `withr::defer()`;
- cleanup attempted even when assertions fail;
- each job responsible only for objects it created.

### Future improvement

The longer-term standard could allocate a unique schema per workflow run:

```text
hades_test_<repository>_<run_id>_<job>
```

That is preferable where the DBMS and permissions allow schema creation. It should not block the first CohortGenerator implementation.

---

# GitHub Actions Proposal

## Separate General Package Checks From Live Database Tests

Do not place the full live database matrix inside the standard cross-operating-system `R CMD check` workflow.

Recommended split:

```text
.github/workflows/R_CMD_check_Hades.yaml
.github/workflows/database-platform-tests.yml
```

### Standard package workflow

Continue running:

- Windows R CMD check;
- macOS R CMD check;
- Ubuntu R CMD check;
- ordinary Eunomia-backed tests;
- R code coverage.

Do not expose all live database credentials to these jobs.

### Live database workflow

Start with one matrix job per DBMS: PostgreSQL. Additional DBMS jobs can be added after the first flow is validated.

The workflow should support pull request, push, scheduled, and manual events, but it does not need to run for every change. Where practical, the workflow should be gated so DBMS jobs are only started when the changed files could affect live database behavior, database-specific helpers, or workflow definitions.

The first version can use repository-level path filters to avoid obviously unrelated runs. If finer control is needed later, job-level conditions can be added so the workflow remains visible but individual DBMS jobs are skipped when they are not relevant.

---

## Initial Static Matrix

Start with a static matrix. It is easier to understand and troubleshoot while the architecture is being validated.

```yaml
name: Database platform tests

on:
  pull_request:
  push:
    branches:
      - main
  schedule:
    - cron: "0 6 * * 1"
  workflow_dispatch:

jobs:
  database-test:
    name: Database test (${{ matrix.dbms }})
    runs-on: ubuntu-latest

    strategy:
      fail-fast: false
      matrix:
        include:
          - dbms: postgresql
            environment: test-postgresql

          - dbms: sql server
            environment: test-sql-server

          - dbms: oracle
            environment: test-oracle

          - dbms: redshift
            environment: test-redshift

          - dbms: spark
            environment: test-spark

          - dbms: bigquery
            environment: test-bigquery

    environment: ${{ matrix.environment }}

    env:
      HADES_TEST_DBMS: ${{ matrix.dbms }}
      HADES_DATABASE_TEST: "true"

    steps:
      - uses: actions/checkout@v4

      - uses: r-lib/actions/setup-r@v2
        with:
          r-version: release

      - uses: actions/setup-java@v4
        with:
          distribution: corretto
          java-version: "8"

      - uses: r-lib/actions/setup-r-dependencies@v2
        with:
          extra-packages: any::testthat

      - name: Run live database tests
        shell: Rscript {0}
        run: |
          testthat::test_file(
            "tests/testthat/test-dbms-platforms.R",
            reporter = "summary"
          )
```

The dependency-installation details should be adapted to the package’s current HADES workflow conventions.

---

## GitHub Environment Isolation

Use one GitHub environment per database platform:

```text
test-postgresql
test-sql-server
test-oracle
test-redshift
test-spark
test-bigquery
```

Each environment should expose only the secrets required for that platform.

Preferred normalized names:

```text
HADES_DB_USER
HADES_DB_PASSWORD
HADES_DB_SERVER
HADES_DB_CONNECTION_STRING
HADES_CDM_SCHEMA
HADES_VOCABULARY_SCHEMA
HADES_WORK_SCHEMA
HADES_DB_KEY_FILE
```

Advantages:

- PostgreSQL jobs do not receive Oracle credentials.
- Environment protection rules can be applied per DBMS.
- Credential rotation is isolated.
- Shared HADES workflows can eventually use one consistent interface.

If GitHub environments are not adopted initially, matrix-specific conditional secret mapping can be used, but it is more cumbersome.

---

## Pull Requests From Forks

GitHub does not expose repository secrets to untrusted fork pull requests.

The workflow policy must explicitly handle this.

Recommended behavior:

- trusted branch pull requests run the required database matrix;
- fork pull requests run ordinary package checks without live database credentials;
- maintainers may trigger a trusted manual or label-driven database run after reviewing the fork;
- missing secrets must not masquerade as a successful required database test.

This policy should be documented in the repository contributing guide.

---

## Static Matrix Validation

Do not dynamically generate the matrix from YAML in the first implementation.

Instead, add a validation script that compares:

- enabled platforms in `hades-database-platforms.yml`;
- platforms present in the GitHub workflow matrix.

A later implementation can generate the matrix automatically.

Possible script:

```text
tools/validate-database-test-matrix.R
```

The script should fail when:

- an enabled required platform has no CI job;
- the workflow includes an undeclared platform;
- a disabled platform remains in the active matrix.

A lightweight first version could maintain a second machine-readable matrix file consumed by both R and GitHub Actions, but this adds an additional configuration source. The preferred long-term approach is YAML-driven matrix generation after the prototype stabilizes.

---

# Local Developer Workflow

A developer should be able to run one live database platform without configuring all others.

## `.Renviron`

Example PostgreSQL configuration:

```text
CDM5_POSTGRESQL_USER=...
CDM5_POSTGRESQL_PASSWORD=...
CDM5_POSTGRESQL_SERVER=...
CDM5_POSTGRESQL_CDM_SCHEMA=...
CDM5_POSTGRESQL_OHDSI_SCHEMA=...
```

Or, using normalized variables:

```text
HADES_DB_USER=...
HADES_DB_PASSWORD=...
HADES_DB_SERVER=...
HADES_CDM_SCHEMA=...
HADES_VOCABULARY_SCHEMA=...
HADES_WORK_SCHEMA=...
```

Secrets should never be committed to the repository.

---

## Run From R

```r
withr::with_envvar(
  c(
    HADES_TEST_DBMS = "postgresql",
    HADES_DATABASE_TEST = "false"
  ),
  testthat::test_file(
    "tests/testthat/test-dbms-platforms.R",
    reporter = "summary"
  )
)
```

Because this is a local run, incomplete environment variables should cause an informative skip rather than a hard failure.

---

## Optional Development Helper

Create a development utility:

```r
testDatabasePlatform <- function(dbms) {
  withr::with_envvar(
    c(
      HADES_TEST_DBMS = dbms,
      HADES_DATABASE_TEST = "false"
    ),
    testthat::test_file(
      "tests/testthat/test-dbms-platforms.R",
      reporter = "summary"
    )
  )
}
```

Possible location:

```text
tools/test-database-platform.R
```

Usage:

```r
source("tools/test-database-platform.R")
testDatabasePlatform("postgresql")
```

This should initially remain a repository development utility rather than an exported CohortGenerator function.

---

## Run From a Shell

### Bash

```bash
HADES_TEST_DBMS=postgresql \
Rscript -e 'testthat::test_file("tests/testthat/test-dbms-platforms.R", reporter = "summary")'
```

### PowerShell

```powershell
$env:HADES_TEST_DBMS = "postgresql"
Rscript -e 'testthat::test_file("tests/testthat/test-dbms-platforms.R", reporter = "summary")'
```

---

# Proposed Repository Changes

## New files

```text
inst/test-config/hades-database-platforms.yml
tests/testthat/helper-database-platforms.R
tools/test-database-platform.R
tools/validate-database-test-matrix.R
.github/workflows/database-platform-tests.yml
```

The matrix validation script may be deferred until after the initial workflow is functioning.

## Modified files

```text
DESCRIPTION
tests/testthat/setup.R
tests/testthat/helper.R
tests/testthat/test-dbms-platforms.R
.github/workflows/R_CMD_check_Hades.yaml
```

### `DESCRIPTION`

Add:

```text
yaml
```

to `Suggests`.

### `tests/testthat/setup.R`

Remove the hard-coded `dbmsPlatforms` vector.

Keep only setup required by the ordinary package test suite.

### `tests/testthat/helper.R`

Move database-platform-specific logic to:

```text
tests/testthat/helper-database-platforms.R
```

Keep unrelated cohort fixture helpers in `helper.R`, or split those into additional focused helper files later.

### `tests/testthat/test-dbms-platforms.R`

Replace the multi-platform loop with a test of the one platform selected through `HADES_TEST_DBMS`.

### Existing `R CMD check` workflow

Remove live database credentials and live database execution from the operating-system matrix.

Continue running normal package checks and code coverage.

### New live database workflow

Add one DBMS per matrix job.

---

# Suggested Implementation Sequence

## Step 1: Add the declaration

- Add `yaml` to `Suggests`.
- Create `inst/test-config/hades-database-platforms.yml`.
- Represent PostgreSQL as the initial supported platform.
- Add a validator for the YAML structure.

## Step 2: Add selected-platform helpers

Implement:

```r
getDatabaseTestConfig()
getSelectedTestDbms()
getDatabasePlatformConfig()
skipIfNoLiveDatabase()
isCiDatabaseTest()
```

At this stage, do not change connection construction.

## Step 3: Refactor connection construction

Break the current `getPlatformConnectionDetails()` function into:

```r
getRequiredDatabaseEnvironmentVariables()
validateDatabaseTestEnvironment()
createPlatformConnectionDetails()
configureSqlRenderTestOptions()
getDatabaseTestContext()
```

Preserve existing platform behavior while removing the large all-in-one conditional function.

## Step 4: Change the test execution model

Replace:

```r
for (dbmsPlatform in dbmsPlatforms)
```

with one selected platform.

Verify local execution for at least PostgreSQL.

## Step 5: Add the GitHub matrix workflow

Start with a single representative platform:

- PostgreSQL.

Once stable, add the remaining DBMS platforms one at a time.

## Step 6: Separate required and scheduled tiers

Decide which databases run:

- on every trusted pull request;
- on pushes to `main`;
- weekly;
- manually.

Record the decision in the YAML `ci` field.

## Step 7: Remove live credentials from general package checks

Keep operating-system compatibility checks independent from database-platform compatibility checks.

## Step 8: Add matrix-declaration validation

Once the structure is stable, ensure the GitHub matrix and package declaration cannot drift.

## Step 9: Generalize beyond CohortGenerator

After CohortGenerator proves the model:

- move common helpers to a shared HADES testing package or infrastructure package;
- define a formal YAML schema;
- standardize environment-variable names;
- provide a reusable GitHub workflow;
- migrate other HADES packages incrementally.

---

# Initial Scope Boundaries

The first CohortGenerator implementation should not attempt to solve all HADES testing concerns.

Defer:

- SQL line-coverage instrumentation;
- SQL asset coverage;
- automatic CDM provisioning;
- CDM content equivalence testing;
- fully dynamic GitHub matrix generation;
- DBMS feature-level exclusion declarations;
- database version compatibility ranges;
- per-test SQL execution tracing;
- a new formal R object model for database contexts;
- organization-wide secret renaming;
- a reusable HADES package before the prototype is validated.

The initial goal is:

> CohortGenerator declares its live database test platforms in a machine-readable YAML file, runs the same database-neutral behavioral test once per selected platform, supports explicit local execution, and uses one isolated GitHub Actions matrix job per DBMS.

---

# Decisions Still Required

## 1. CI frequency

Which platforms should run:

- on every trusted pull request;
- only on `main`;
- on a weekly schedule;
- through manual dispatch?

A tiered approach is likely appropriate because some external database tests may be slow, costly, rate-limited, or less reliable.

## 2. Meaning of the declaration

Should the YAML list:

- only platforms used in automated CI; or
- all platforms the package claims to support?

Recommendation: list all supported platforms and use the separate `ci` field to describe automation frequency.

## 3. Environment-variable standard

Should CohortGenerator:

- retain only the current `CDM5_*` and `CDM_*` variables; or
- introduce normalized `HADES_*` variables with backward-compatible fallbacks?

Recommendation: support normalized variables first and legacy aliases second, without requiring immediate secret migration.

## 4. Writable schema concurrency

Can multiple jobs safely share the same writable OHDSI schema?

Questions to resolve:

- Are randomized table names sufficient?
- Do all derived CohortGenerator tables inherit the unique prefix?
- Can parallel tests interfere through shared metadata tables?
- Can the infrastructure provide a unique schema per workflow job?

The initial prototype can retain randomized table names, but concurrency should be explicitly tested.

## 5. Pull requests from forks

Determine how maintainers will authorize live database tests for forked contributions.

Possible approaches:

- manual `workflow_dispatch`;
- trusted branch created by a maintainer;
- label-triggered trusted workflow;
- database tests only after merge to a protected integration branch.

---

# Recommended First Codex Task

A useful first implementation task for Codex would be:

> Refactor CohortGenerator’s live DBMS test infrastructure so that supported platforms are declared in `inst/test-config/hades-database-platforms.yml`, one platform is selected using `HADES_TEST_DBMS`, and `tests/testthat/test-dbms-platforms.R` runs only that selected platform. Preserve all current DatabaseConnector connection behavior and existing environment-variable names. Add unit tests for YAML parsing and platform selection, but do not yet modify GitHub Actions.

This isolates the R package changes from CI changes and allows local validation before restructuring the workflows.

A second task would then be:

> Add `.github/workflows/database-platform-tests.yml` with one matrix job per enabled database platform, set `HADES_TEST_DBMS` and `HADES_DATABASE_TEST`, and remove live database credentials from the general R CMD check workflow. Begin with PostgreSQL and SQL Server, then expand the matrix after those jobs are stable.

---

# Acceptance Criteria for the Initial Prototype

The initial implementation is successful when:

1. The supported database platforms are declared in YAML.
2. No hard-coded platform vector remains in `tests/testthat/setup.R`.
3. Setting `HADES_TEST_DBMS=postgresql` runs only PostgreSQL live tests.
4. A local developer with incomplete credentials receives a clear skip message listing missing variables.
5. A required GitHub database job with missing credentials fails.
6. Each GitHub database job is named for its DBMS.
7. Each job opens a real DatabaseConnector connection.
8. The shared CohortGenerator behavior test runs unchanged across platforms.
9. Cleanup occurs when tests pass or fail.
10. Ordinary Eunomia-backed tests and operating-system checks continue independently.
11. No database passwords, servers, or schemas appear in the YAML file.
12. Disabled platforms include a structured reason.
