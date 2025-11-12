# Get Exclude On Index Subset Definition Ids

Get the exclusion on index subset definition ids from a cohort
definition set (if any have been added) Useful if keeping track in a
script with complex business logic around what a cohort definition is
for

## Usage

``` r
getExcludeOnIndexSubsetDefinitionIds(cohortDefinitionSet)
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
