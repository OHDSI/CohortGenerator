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