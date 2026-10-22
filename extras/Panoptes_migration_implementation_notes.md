# Panoptes migration implementation notes

## Implemented scope

- Added `Panoptes (>= 0.1.0)` as a test dependency pinned to the released
  `OHDSI/Panoptes@v0.1.0` tag.
- Replaced the prototype database declaration with the Panoptes schema.
- Refactored the live behavior test to use a Panoptes test context while
  retaining CohortGenerator's fixtures, assertions, and database cleanup.
- Replaced the copied infrastructure tests with consumer integration tests.
- Removed the copied database-platform helper and the remaining connection
  constructor from `helper.R`.
- Normalized live-test environment variables, including the separate
  PostgreSQL results-data-model test.
- Consolidated live CI into one pull-request/manual workflow containing every
  enabled external platform, with cross-platform Java setup, explicit JDBC
  provisioning, and workflow/declaration validation.
- Removed the obsolete secondary R CMD check workflow.
- Confirmed that Panoptes 0.1.0 contains the source-package discovery fix found
  during the initial CohortGenerator pilot.

## Panoptes 0.1.0 breaking changes applied

- Removed every `ci` field from the platform declaration. Panoptes now rejects
  that prototype field as unknown.
- Updated `validateDatabaseTestWorkflow()` calls to its two-argument API.
- Replaced split consumer assertions with assertions over all enabled
  external platforms.
- Consolidated the former split workflow rows into the complete
  `database-platform-tests.yml` matrix.

## Implementation decisions

- No legacy environment-variable bridge was retained. Local and CI live tests
  must use the normalized `HADES_*` interface documented by Panoptes.
- The immutable `v0.1.0` release tag is used instead of a moving development
  branch.
- All eight external platforms run for trusted pull requests and can also be
  started manually.
- Live pull-request jobs are skipped for fork-originated pull requests so fork
  code is not treated as authorized to use protected environments.
- BigQuery uses `windows-latest`, matching the Panoptes declaration.

## Repository configuration still required

Each GitHub environment named in
`inst/test-config/hades-database-platforms.yml` must expose the same normalized
secret names as applicable: `HADES_DB_USER`, `HADES_DB_PASSWORD`,
`HADES_DB_SERVER`, `HADES_DB_CONNECTION_STRING`, `HADES_DB_KEY_FILE`,
`HADES_CDM_SCHEMA`, `HADES_VOCABULARY_SCHEMA`, and `HADES_WORK_SCHEMA`.

The live database jobs cannot be exercised locally without those credentials.
The first trusted live workflow should confirm all eight matrix jobs. Diagnose
the rollout beginning with PostgreSQL, then SQL Server and Oracle, followed by
the remaining platforms.

## Verification results

- The Panoptes declaration and complete workflow matrix validate successfully.
- All CohortGenerator test files parse successfully.
- The focused Panoptes integration suite passes; the live behavior test skips
  as expected when no DBMS is selected.
- A selected local PostgreSQL run without credentials skips and lists only the
  missing normalized variable names.
- A required live-CI PostgreSQL run without credentials fails before connection
  construction, as intended.
- `R CMD build` succeeds and `R CMD check --ignore-vignettes` completes with
  zero errors, warnings, or notes. Vignettes were not rebuilt because Pandoc is
  unavailable in the local environment.
- The exact Panoptes v0.1.0 tag's full test suite passes.
- CohortGenerator's repository-wide lint still reports the branch's existing
  lint backlog. No new lint was identified in the migration code; the focused
  lint output contains existing helper/global-binding and pipe-style findings.

Running the complete suite directly with `devtools::test()` also reaches an
existing failure in the unchanged SQLite path of `test-ResultsDataModel.R`:
`!migrations$executed` has an invalid argument type. The package check is clean
because that test uses `skip_on_cran()`. This is not caused by the Panoptes
migration, but should be tracked separately if local non-CRAN test runs are
expected to be completely green.

## Follow-up questions

1. Have the normalized secrets been created in all eight protected GitHub
   environments?
2. Does the organization want manual approval on every live-test environment?
3. Should the pre-existing SQLite results-model migration failure be addressed
   in this branch or in a separate issue?
