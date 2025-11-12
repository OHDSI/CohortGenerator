# Add Restriction Subset Definition

Utility pattern for creating cohort subset definitions as a standard
approach for indicated drugs. Restriction subset definitions are twins
of indication definitions. They should apply the same core properties to
a base exposure cohort (i.e. study dates, required prior observation
time, ages, gender) as indications but, crucially, they do not require
history of any prior condition(s).

This is useful in the context of comparing drug exposure + indication
population, to population as a whole.

The preferred use of this function is to create this in conjunction with
the target population.

## Usage

``` r
addRestrictionSubsetDefinition(
  cohortDefinitionSet,
  targetCohortIds,
  subsetDefinitionId,
  subsetDefinitionName,
  subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
  genderConceptIds = NULL,
  ageMin = NULL,
  ageMax = NULL,
  studyStartDate = NULL,
  studyEndDate = NULL,
  requiredPriorObservationTime = 365,
  requiredFollowUpTime = 1
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

- targetCohortIds:

  Set of integer cohort IDs. Must be within the cohort definition set.

- subsetDefinitionId:

  Unique integer Id of the subset definition

- subsetDefinitionName:

  name of the subset definition (used in resulting cohort definitions)

- subsetCohortNameTemplate:

  template string format for naming resulting cohorts

- genderConceptIds:

  Gender concepts to require

- ageMin:

  Minimum age at target index.

- ageMax:

  Maximum age at target index.

- studyStartDate:

  Exclude patients with index prior to this date (format "%Y%m%d").

- studyEndDate:

  Exclude patients with index after this date (format "%Y%m%d").

- requiredPriorObservationTime:

  Observation time prior to index; default 365.

- requiredFollowUpTime:

  Observation time after index; default 1.

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

# Restrinct to first occurrence of cohort
res <- addRestrictionSubsetDefinition(
  cohortDefinitionSet = initialSet,
  targetCohortIds = c(1, 2),
  subsetDefinitionId = 20
)

print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])

# Get all subset definitions that were created using the addRestrictionSubsetDefinition:
subsetDefinitionId <- getRestrictionSubsetDefinitionIds(res)

# Filter the cohortDefinitionSet to those cohorts defined using an restriction subset definition:
newCohorts <- res |>
  dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
  dplyr::select(cohortId, cohortName, subsetParent, isSubset)
print(newCohorts)
} # }
```
