# Add Cohort template definition to cohort set

Adds a cohort template definition to an existing cohort definition set
or creates one if none provided

## Usage

``` r
addCohortTemplateDefintion(
  cohortDefinitionSet = createEmptyCohortDefinitionSet(),
  cohortTemplateDefintion
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

- cohortTemplateDefintion:

  An instance of CohortTemplateDefinition (or subclass). See \[@seealso
  \[createCohortTemplateDefinition()\]\].
