# A definition of subset functions to be applied to a set of cohorts

A definition of subset functions to be applied to a set of cohorts

## Usage

``` r
createCohortSubsetOperator(
  name = NULL,
  cohortIds,
  cohortCombinationOperator,
  negate,
  windows = list(),
  startWindow = NULL,
  endWindow = NULL
)
```

## Arguments

- name:

  optional name of operator

- cohortIds:

  integer - set of cohort ids to subset to

- cohortCombinationOperator:

  "any" or "all" if using more than one cohort id allow a subject to be
  in any cohort or require that they are in all cohorts in specified
  windows

- negate:

  The opposite of this definition - include patients who do NOT meet the
  specified criteria

- windows:

  A list of time windows to use to evaluate subset cohorts in relation
  to the target cohorts. The logic is to always apply these windows with
  logical AND conditions. See \[@seealso
  \[createSubsetCohortWindow()\]\] for more details on how to create
  these windows.

- startWindow:

  DEPRECATED: Use \`windows\` instead.

- endWindow:

  DEPRECATED: Use \`windows\` instead.

## Value

a CohortSubsetOperator instance

## See also

Other subsets:
[`createDemographicSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createDemographicSubsetOperator.md),
[`createLimitSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createLimitSubsetOperator.md)
