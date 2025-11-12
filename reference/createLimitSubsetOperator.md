# Create Limit Subset Operator

Subset cohorts using specified limit criteria

## Usage

``` r
createLimitSubsetOperator(
  name = NULL,
  priorTime = 0,
  followUpTime = 0,
  minimumCohortDuration = 0,
  maximumCohortDuration = NULL,
  limitTo = "all",
  calendarStartDate = NULL,
  calendarEndDate = NULL
)
```

## Arguments

- name:

  Name of operation

- priorTime:

  Required prior observation window (specified as a positive integer)

- followUpTime:

  Required post observation window (specified as a positive integer)

- minimumCohortDuration:

  Required cohort duration length (specified as a positive integer)

- maximumCohortDuration:

  Optional: maximum cohort duration length (specified as a positive
  integer), defaults to NULL

- limitTo:

  character one of: "firstEver" - only first entry in patient history
  "earliestRemaining" - only first entry after washout set by priorTime
  "latestRemaining" - the latest remaining after washout set by
  followUpTime "lastEver" - only last entry in patient history inside

  Note, when using firstEver and lastEver with follow up and washout,
  patients with events outside this will be censored. The "firstEver"
  and "lastEver" are applied first. The "earliestRemaining" and
  "latestRemaining" are applied after all other limit criteria are
  applied (i.e. after applying prior/post time and calendar time).

- calendarStartDate:

  End date to allow periods (e.g. 2020/1/1/)

- calendarEndDate:

  Start date to allow period (e.g. 2015/1/1)

## See also

Other subsets:
[`createCohortSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortSubsetOperator.md),
[`createDemographicSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createDemographicSubsetOperator.md)
