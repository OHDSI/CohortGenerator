# Create cohort template to union multiple cohorts

This is a union between all cohorts within a specified set of ids. If an
individual has multiple overlapping eras, they will be merged into a
single time window.

Distinct eras will be mapped to the same cohort id but remain distinct.
For example:

“\` A: \[——–\] B: \[–\] C: \[——-\] “\` Becomes: “\` A U B U C: \[————–\]
“\`

And “\` A: \[——–\] B: \[——-\] “\` Becomes “\` A U B: \[——–\] \[——-\] “\`
It is never allowed to have multiple overlapping eras for the same
individual within a cohort

## Usage

``` r
createUnionCohortTemplate(cohortIds, cohortName, unionCohortId)
```

## Arguments

- cohortIds:

  A vector of \`cohort_definition_id\` values for the input cohorts.

- cohortName:

  The Name of the resulting cohort

- unionCohortId:

  The \`cohort_definition_id\` for the resulting union cohort.
