# Add exclude on index subset definition

The purpose of this subset recipe is to exclude all individuals if their
index aligns with the specified exclusion cohort ids. If the index date
of the exclusionCohortIds aligns with the targetCohortIds (or it lies
within some relative window of the target cohort start date) then they
will be excluded from the resulting sub population.

This may be used in situations where an outcome cohort may contain
individuals treated for a target medication, complicating calculation of
incidence rates.

## Usage

``` r
addExcludeOnIndexSubsetDefinition(
  cohortDefinitionSet,
  subsetDefinitionName,
  subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
  targetCohortIds,
  exclusionCohortIds,
  exclusionWindow = 0,
  subsetDefinitionId,
  cohortCombinationOperator = "any"
)
```

## Arguments

- cohortDefinitionSet:

  The `cohortDefinitionSet` argument must be a data frame with the
  following columns:

  cohortId

  :   The unique integer identifier of the cohort

  cohortName

  :   The cohort's name

  sql

  :   The OHDSI-SQL used to generate the cohort

  Optionally, this data frame may contain:

  json

  :   The Circe JSON representation of the cohort

- subsetDefinitionName:

  name of the subset definition (used in resulting cohort definitions)

- subsetCohortNameTemplate:

  template string format for naming resulting cohorts

- targetCohortIds:

  Set of integer cohort IDs. Must be within the cohort definition set.

- exclusionCohortIds:

  cohort ids to exclude members of target from

- exclusionWindow:

  Days Default is 0 (target index date). by changing this you can adjust
  the period around target index for which you would exclude members.

- subsetDefinitionId:

  Unique integer Id of the subset definition

- cohortCombinationOperator:

  Logic for multiple indication cohort IDs: any (default) or all.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CohortGenerator)

initialSet <- getCohortDefinitionSet(
  settingsFileName = "testdata/name/Cohorts.csv",
  jsonFolder = "testdata/name/cohorts",
  sqlFolder = "testdata/name/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortName"),
  packageName = "CohortGenerator",
  verbose = FALSE
)

print(initialSet[, c("cohortId", "cohortName")])

# Subset cohorts 1 & 2 by an "indication" cohort 3:
res <- addExcludeOnIndexSubsetDefinition(
  cohortDefinitionSet = initialSet,
  targetCohortIds = c(1, 2),
  exclusionCohortIds = c(3),
  subsetDefinitionId = 20,
  subsetDefinitioName = "Exclude on index if in cohort 3"
)

print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])

# Get all subset definitions that were created using the addExcludeOnIndexSubsetDefinition:
subsetDefinitionId <- getExcludeOnIndexSubsetDefinitionIds(res)

# Filter the cohortDefinitionSet to those cohorts defined using an exclusion subset definition:
newCohorts <- res |>
  dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
  dplyr::select(cohortId, cohortName, subsetParent, isSubset)
print(newCohorts)
} # }
```
