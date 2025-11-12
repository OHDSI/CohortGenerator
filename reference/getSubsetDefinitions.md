# Get cohort subset definitions from a cohort definition set

Get the subset definitions (if any) applied to a cohort definition set.
Note that these subset definitions are a copy of those applied to the
cohort set. Modifying these definitions will not modify the base cohort
set. To apply a modification, reapply the subset definition to the
cohort definition set data.frame with `addCohortSubsetDefinition` with
\`overwriteExisting = TRUE\`.

## Usage

``` r
getSubsetDefinitions(cohortDefinitionSet)
```

## Arguments

- cohortDefinitionSet:

  A valid cohortDefinitionSet

## Value

list of cohort subset definitions or empty list
