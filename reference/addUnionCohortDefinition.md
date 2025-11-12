# Add union cohort definition to cohort definition set

This utility function adds the union of any two or more cohort ids to
the cohort definition set with a new id and name.

If a name parameter is not provided this will be auto generated as the
union of the provided cohort id

## Usage

``` r
addUnionCohortDefinition(
  cohortDefinitionSet,
  cohortIds,
  cohortName,
  unionCohortId
)
```

## Arguments

- cohortDefinitionSet:

  cohort definition set

- cohortIds:

  A vector of \`cohort_definition_id\` values for the input cohorts.

- cohortName:

  The Name of the resulting cohort

- unionCohortId:

  The \`cohort_definition_id\` for the resulting union cohort.
