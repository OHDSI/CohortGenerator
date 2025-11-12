# Get Cohort Inclusion Rules from a cohort definition set

This function returns a data frame of the inclusion rules defined in a
cohort definition set.

## Usage

``` r
getCohortInclusionRules(cohortDefinitionSet)
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
