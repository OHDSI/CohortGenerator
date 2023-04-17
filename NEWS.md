CohortGenerator 0.8.0
=======================
- New feature: cohort subsetting (Issue #67).
- Removes the evaluation of ROhdsiWebApi code in vignettes (Issue #70)
- Basic tests for different database platforms (#71)

CohortGenerator 0.7.0
=======================
- Fixes data type issue for Google Big Query (Issue #51).
- Removes the `databaseId` field from the cohort inclusion table (Issue #52)
- Adds the ability to generate negative control outcome cohorts for use in population-level estimation. (Issue #9)

CohortGenerator 0.6.0
=======================
- Add more flexibility when reading/writing CSV files including appending (Issue #44), flags for disabling warnings (Issue #38) and removing unhelpful warnings (Issue #43).
- Added better error handling to `saveCohortDefinitionSet` (Issue #25)
- Add better handling of column name casing in CSV files that hold cohort settings. (Issue #37)
- Added functions to check if a data.frame conforms to a cohortDefinitionSet (Issue #21)

CohortGenerator 0.5.0
=======================
- Allow for specification of the database_id in export methods. (Issue #18)
- `getCohortStats` function allows for exporting cohort statistics into data frames (Pull Request #24)
- Add utility methods for reading/writing CSV files. (Issue #16)

CohortGenerator 0.4.0
=======================
- Update dependency versions in DESCRIPTION to resolve issues when using older versions of readr/stringi. (Issue #13)
- Breaking change: change the defaults for saving/getting a cohortDefinitionSet to/from the file system. (Issue #16)
- `getCohortCounts` now supports an optional parameter `cohortDefinitionSet` which will join the cohort counts with the cohort definition set when desired (Issue #14)

CohortGenerator 0.3.0
=======================
- Added `getCohortDefintionSet` function for retrieving a cohort definition set from either a package or the file system
- Re-factored `saveCohortDefinitionSet` to remove the `settingsFolder`. The `settingsFileName` should include the path to the file.
- Bug fixes

CohortGenerator 0.2.0
=======================

- Renamed `createCohortTable` to `createCohortTables` to include all cohort statistics tables
- Renamed `createEmptyCohortSet` to `createEmptyCohortDefinitionSet` to make this consistent with ROhdsiWebApi and CohortDiagnostics
- Added `dropCohortStatsTables` function for removing cohort statistics tables
- Added `exportCohortStatsTables` function for exporting cohort statistics to CSV files
- Added `getCohortTableNames` function to define the list of cohort table names to create
- Added `insertInclusionRuleNames` for inserting the inclusion rule names into the cohort stats table
- Added `saveCohortDefinitionSet` to save the cohort definition set to the file system for use by study packages
- Added a vignette and updated documentation
- Remove CirceR dependency and related functions

CohortGenerator 0.1.1
=======================

Fixing documentation

CohortGenerator 0.1.0
=======================

Updating function calls to use "generate" instead of "instantiate"

CohortGenerator 0.0.1
=======================

Initial version