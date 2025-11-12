# Add Indication Subset Definition

Utility pattern for creating an indication subset from a set of target
cohorts. The approach applies this subset definition to an exposure
(target cohort) or set of exposures (multiple target cohorts), requiring
the individual to have a history of the indication cohort overlapping
the start of the first exposure. The first exposure must have the
\`requiredPriorObservationTime\` and \`requiredFollowUpTime\`. If
specified, the first exposure must also fall within the
\`studyStartDate\` and \`studyEndDate\` and also meet the age and gender
criteria.

Additionally, the R attribute of "indicationSubsetDefinitions" is
attached to the cohort definition set. This can be obtained by calling
\`getIndicationSubsetDefinitionIds\`, which should return the set of
subset definition ids that are associated with indications.

## Usage

``` r
addIndicationSubsetDefinition(
  cohortDefinitionSet,
  targetCohortIds,
  indicationCohortIds,
  subsetDefinitionId,
  subsetDefinitionName,
  subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
  cohortCombinationOperator = "any",
  lookbackWindowStart = -99999,
  lookbackWindowEnd = 0,
  lookForwardWindowStart = 0,
  lookForwardWindowEnd = 99999,
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

- indicationCohortIds:

  Set of integer cohort IDs. Must be within the cohort definition set.

- subsetDefinitionId:

  Unique integer Id of the subset definition

- subsetDefinitionName:

  name of the subset definition (used in resulting cohort definitions)

- subsetCohortNameTemplate:

  template string format for naming resulting cohorts

- cohortCombinationOperator:

  Logic for multiple indication cohort IDs: any (default) or all.

- lookbackWindowStart:

  Start of lookback period.

- lookbackWindowEnd:

  End of lookback period.

- lookForwardWindowStart:

  When the indication can end relative to index; default is 0.

- lookForwardWindowEnd:

  When the indication can end relative to index; default is 9999.

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

# Subset cohorts 1 & 2 by an "indication" cohort 3:
res <- addIndicationSubsetDefinition(
  cohortDefinitionSet = initialSet,
  targetCohortIds = c(1, 2),
  indicationCohortIds = c(3),
  subsetDefinitionId = 10
)

print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])

# Get all subset definitions that were created using the addIndicationSubsetDefinition:
subsetDefinitionId <- getIndicationSubsetDefinitionIds(res)

# Filter the cohortDefinitionSet to those cohorts defined using an indication subset definition:
newCohorts <- res |>
  dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
  dplyr::select(cohortId, cohortName, subsetParent, isSubset)
print(newCohorts)
} # }
```
