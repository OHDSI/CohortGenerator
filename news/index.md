# Changelog

## CohortGenerator 1.1.0

New Features

- Adds functions and documentation for computing attrition for cohorts
  and subsetted cohorts
  ([\#123](https://github.com/OHDSI/CohortGenerator/issues/123),
  [\#79](https://github.com/OHDSI/CohortGenerator/issues/79))

## CohortGenerator 1.0.2

CRAN release: 2026-02-10

Bug Fixes

- Fix bug where `negate` parameter of `createCohortSubsetOperator` was
  not passed properly to the SQL statement
  ([\#264](https://github.com/OHDSI/CohortGenerator/issues/264))
- Fix deprecation messages
  ([\#263](https://github.com/OHDSI/CohortGenerator/issues/263))
- Fix issue with unboxing nulls
  ([\#262](https://github.com/OHDSI/CohortGenerator/issues/262))

## CohortGenerator 1.0.1

CRAN release: 2025-11-17

Bug Fixes

- Remove calls to `lifecycle` and add unit tests

## CohortGenerator 1.0.0

CRAN release: 2025-11-12

New Features

- SQL cohorts as first class citizens - SQL templates for non-standard
  cohorts or large, bulk operations
  ([\#133](https://github.com/OHDSI/CohortGenerator/issues/133),
  [\#247](https://github.com/OHDSI/CohortGenerator/issues/247))
- Remove file-based incremental operations. Creation of database
  cohort_checksum tables that enables verification of generated cohorts
  and incremental execution in distributed environments
  ([\#206](https://github.com/OHDSI/CohortGenerator/issues/206),
  [\#131](https://github.com/OHDSI/CohortGenerator/issues/131),
  [\#254](https://github.com/OHDSI/CohortGenerator/issues/254))
- Abbreviated logging for cohorts already generated
  ([\#113](https://github.com/OHDSI/CohortGenerator/issues/113))
- Added `maximumChortDuration` to the LimitSubset operator
  ([\#240](https://github.com/OHDSI/CohortGenerator/issues/240))
- Added “recipe” functions for creating indication and restriction
  subsets ([\#209](https://github.com/OHDSI/CohortGenerator/issues/209))
- Deprecate subset operator function names and add `operator` suffix
  ([\#233](https://github.com/OHDSI/CohortGenerator/issues/233))
- Removes default ‘print friendly’ long text of subgroup names
  ([\#135](https://github.com/OHDSI/CohortGenerator/issues/135),
  [\#231](https://github.com/OHDSI/CohortGenerator/issues/231))
- Add pretty print to subset definitions that includes SQL and logic
  definitions
  ([\#218](https://github.com/OHDSI/CohortGenerator/issues/218))

Bug Fixes

- Limit subset operator produces broken SQL if R `date` of length 0 is
  passed instead of NULL
  ([\#252](https://github.com/OHDSI/CohortGenerator/issues/252))
- Remove warning around finalize
  ([\#242](https://github.com/OHDSI/CohortGenerator/issues/242))

## CohortGenerator 0.12.2

CRAN release: 2025-09-11

- Updates unit tests to use lowercase field names
  ([\#237](https://github.com/OHDSI/CohortGenerator/issues/237))

## CohortGenerator 0.12.1

CRAN release: 2025-09-05

Bug Fixes - Fix logical issue with null minimum cohort duration

## CohortGenerator 0.12.0

CRAN release: 2025-07-31

New Features

- Backwards compatible extension to CohortSubsetOperators and
  cohortSubsetWindows to allow windowing to be logic of any length
- Include observation table when creating negative control cohorts
  ([\#198](https://github.com/OHDSI/CohortGenerator/issues/198))
- Improvements to cohort subset documentation
  ([\#199](https://github.com/OHDSI/CohortGenerator/issues/199))
- Add `negate` parameter to createSubsetCohortWindow function
  ([\#217](https://github.com/OHDSI/CohortGenerator/issues/217))

Bug Fixes - Remove RJSONIO from dependency list
([\#202](https://github.com/OHDSI/CohortGenerator/issues/202)) - Upgrade
GitHub Actions to use Ubuntu 22.04
([\#204](https://github.com/OHDSI/CohortGenerator/issues/204)) - Add
missing `database_id` as primary key of `cg_cohort_censor_stats`
([\#215](https://github.com/OHDSI/CohortGenerator/issues/215)) -
saveIncremental prevents saving empty files
([\#212](https://github.com/OHDSI/CohortGenerator/issues/212)) - Subset
identifier expressions are not deserialized
([\#225](https://github.com/OHDSI/CohortGenerator/issues/225))

## CohortGenerator 0.11.2

CRAN release: 2024-09-30

- Ensure temp tables are dropped before creating them
  ([\#188](https://github.com/OHDSI/CohortGenerator/issues/188))

## CohortGenerator 0.11.1

CRAN release: 2024-09-15

- CohortGenerator added to CRAN
  ([\#77](https://github.com/OHDSI/CohortGenerator/issues/77))

## CohortGenerator 0.11.0

New Features

- Add support for minimum cell count
  ([\#176](https://github.com/OHDSI/CohortGenerator/issues/176))

Bug Fixes

- Multiple calls to export stats causing duplicates in cohort inclusion
  file ([\#179](https://github.com/OHDSI/CohortGenerator/issues/179))
- Updates to subset documentation
  ([\#180](https://github.com/OHDSI/CohortGenerator/issues/180),
  [\#181](https://github.com/OHDSI/CohortGenerator/issues/181))
- Negative control outcome generation bug
  ([\#177](https://github.com/OHDSI/CohortGenerator/issues/177))

## CohortGenerator 0.10.0

New Features

- Add `runCohortGeneration` function (Issue
  [\#165](https://github.com/OHDSI/CohortGenerator/issues/165))
- Adopt ResultModelManager for handling results data models & uploading.
  Extend results data model to include information on cohort
  subsets([\#154](https://github.com/OHDSI/CohortGenerator/issues/154),
  [\#162](https://github.com/OHDSI/CohortGenerator/issues/162))
- Remove REMOTES entries for CirceR and Eunomia which are now in CRAN
  ([\#145](https://github.com/OHDSI/CohortGenerator/issues/145))
- Unit tests now running on all OHDSI DB Platforms
  ([\#151](https://github.com/OHDSI/CohortGenerator/issues/151))

Bug Fixes

- Negation of cohort subset operator must join on `subject_id` AND
  `start_date`
  ([\#167](https://github.com/OHDSI/CohortGenerator/issues/167))
- Allow integer as cohort ID
  ([\#146](https://github.com/OHDSI/CohortGenerator/issues/146))
- Use native messaging functions for output vs. ParallelLogger
  ([\#97](https://github.com/OHDSI/CohortGenerator/issues/97))
- Prevent upload of inclusion rule information
  ([\#78](https://github.com/OHDSI/CohortGenerator/issues/78))
- Expose `colTypes` when working with .csv files
  ([\#59](https://github.com/OHDSI/CohortGenerator/issues/59))
- Remove `bit64` from package (mostly)
  ([\#152](https://github.com/OHDSI/CohortGenerator/issues/152))
- Updated documentation for cohort subset negate feature
  ([\#111](https://github.com/OHDSI/CohortGenerator/issues/111))

## CohortGenerator 0.9.0

- Random sample functionality (for development only) (Issue
  [\#129](https://github.com/OHDSI/CohortGenerator/issues/129))
- Incremental mode for negative control cohort generation (Issue
  [\#137](https://github.com/OHDSI/CohortGenerator/issues/137))
- Fixes getCohortCounts() if cohortIds is not specified, but
  cohortDefinitionSet is. (Issue
  [\#136](https://github.com/OHDSI/CohortGenerator/issues/136))
- Add cohort ID to generation output messages (Issue
  [\#132](https://github.com/OHDSI/CohortGenerator/issues/132))
- Add databaseId to output of getStatsTable() (Issue
  [\#116](https://github.com/OHDSI/CohortGenerator/issues/116))
- Prevent duplicate cohort IDs in cohortDefinitionSet (Issue
  [\#130](https://github.com/OHDSI/CohortGenerator/issues/130))
- Fix cohort stats query for Oracle (Issue
  [\#143](https://github.com/OHDSI/CohortGenerator/issues/143))
- Ensure databaseId applied to all returned cohort counts (Issue
  [\#144](https://github.com/OHDSI/CohortGenerator/issues/144))
- Preserve backwards compatibility if cohort sample table is not in the
  list of cohort table names (Issue
  [\#147](https://github.com/OHDSI/CohortGenerator/issues/147))

## CohortGenerator 0.8.1

- Include cohorts with 0 people in cohort counts (Issue
  [\#91](https://github.com/OHDSI/CohortGenerator/issues/91)).
- Use numeric for cohort ID (Issue
  [\#98](https://github.com/OHDSI/CohortGenerator/issues/98))
- Allow big ints for target pairs
  ([\#103](https://github.com/OHDSI/CohortGenerator/issues/103))
- Pass `tempEmulationSchema` when creating negative control cohorts
  ([\#104](https://github.com/OHDSI/CohortGenerator/issues/104))
- Target CDM v5.4 for unit tests
  ([\#119](https://github.com/OHDSI/CohortGenerator/issues/119))
- Fix for subset references
  ([\#115](https://github.com/OHDSI/CohortGenerator/issues/115))
- Allow for subset cohort name templating
  ([\#118](https://github.com/OHDSI/CohortGenerator/issues/118))
- Allow all entries with limit operator and do not require \> 0 days
  follow up
  ([\#112](https://github.com/OHDSI/CohortGenerator/issues/112))

## CohortGenerator 0.8.0

- New feature: cohort subsetting (Issue
  [\#67](https://github.com/OHDSI/CohortGenerator/issues/67)).
- Removes the evaluation of ROhdsiWebApi code in vignettes (Issue
  [\#70](https://github.com/OHDSI/CohortGenerator/issues/70))
- Basic tests for different database platforms
  ([\#71](https://github.com/OHDSI/CohortGenerator/issues/71))

## CohortGenerator 0.7.0

- Fixes data type issue for Google Big Query (Issue
  [\#51](https://github.com/OHDSI/CohortGenerator/issues/51)).
- Removes the `databaseId` field from the cohort inclusion table (Issue
  [\#52](https://github.com/OHDSI/CohortGenerator/issues/52))
- Adds the ability to generate negative control outcome cohorts for use
  in population-level estimation. (Issue
  [\#9](https://github.com/OHDSI/CohortGenerator/issues/9))

## CohortGenerator 0.6.0

- Add more flexibility when reading/writing CSV files including
  appending (Issue
  [\#44](https://github.com/OHDSI/CohortGenerator/issues/44)), flags for
  disabling warnings (Issue
  [\#38](https://github.com/OHDSI/CohortGenerator/issues/38)) and
  removing unhelpful warnings (Issue
  [\#43](https://github.com/OHDSI/CohortGenerator/issues/43)).
- Added better error handling to `saveCohortDefinitionSet` (Issue
  [\#25](https://github.com/OHDSI/CohortGenerator/issues/25))
- Add better handling of column name casing in CSV files that hold
  cohort settings. (Issue
  [\#37](https://github.com/OHDSI/CohortGenerator/issues/37))
- Added functions to check if a data.frame conforms to a
  cohortDefinitionSet (Issue
  [\#21](https://github.com/OHDSI/CohortGenerator/issues/21))

## CohortGenerator 0.5.0

- Allow for specification of the database_id in export methods. (Issue
  [\#18](https://github.com/OHDSI/CohortGenerator/issues/18))
- `getCohortStats` function allows for exporting cohort statistics into
  data frames (Pull Request
  [\#24](https://github.com/OHDSI/CohortGenerator/issues/24))
- Add utility methods for reading/writing CSV files. (Issue
  [\#16](https://github.com/OHDSI/CohortGenerator/issues/16))

## CohortGenerator 0.4.0

- Update dependency versions in DESCRIPTION to resolve issues when using
  older versions of readr/stringi. (Issue
  [\#13](https://github.com/OHDSI/CohortGenerator/issues/13))
- Breaking change: change the defaults for saving/getting a
  cohortDefinitionSet to/from the file system. (Issue
  [\#16](https://github.com/OHDSI/CohortGenerator/issues/16))
- `getCohortCounts` now supports an optional parameter
  `cohortDefinitionSet` which will join the cohort counts with the
  cohort definition set when desired (Issue
  [\#14](https://github.com/OHDSI/CohortGenerator/issues/14))

## CohortGenerator 0.3.0

- Added `getCohortDefintionSet` function for retrieving a cohort
  definition set from either a package or the file system
- Re-factored `saveCohortDefinitionSet` to remove the `settingsFolder`.
  The `settingsFileName` should include the path to the file.
- Bug fixes

## CohortGenerator 0.2.0

- Renamed `createCohortTable` to `createCohortTables` to include all
  cohort statistics tables
- Renamed `createEmptyCohortSet` to `createEmptyCohortDefinitionSet` to
  make this consistent with ROhdsiWebApi and CohortDiagnostics
- Added `dropCohortStatsTables` function for removing cohort statistics
  tables
- Added `exportCohortStatsTables` function for exporting cohort
  statistics to CSV files
- Added `getCohortTableNames` function to define the list of cohort
  table names to create
- Added `insertInclusionRuleNames` for inserting the inclusion rule
  names into the cohort stats table
- Added `saveCohortDefinitionSet` to save the cohort definition set to
  the file system for use by study packages
- Added a vignette and updated documentation
- Remove CirceR dependency and related functions

## CohortGenerator 0.1.1

Fixing documentation

## CohortGenerator 0.1.0

Updating function calls to use “generate” instead of “instantiate”

## CohortGenerator 0.0.1

Initial version
