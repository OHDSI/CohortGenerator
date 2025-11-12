# Create a relative time window for cohort subset operations

This function is used to create a relative time window for cohort subset
operations. The cohort window allows you to define an interval of time
relative to the target cohort's start/end date and the subset cohort's
start/end date.

## Usage

``` r
createSubsetCohortWindow(
  startDay,
  endDay,
  targetAnchor,
  subsetAnchor = NULL,
  negate = FALSE
)
```

## Arguments

- startDay:

  The start day for the time window

- endDay:

  The end day for the time window

- targetAnchor:

  To anchor using the target cohort's start date or end date. The
  parameter is specified as 'cohortStart' or 'cohortEnd'.

- subsetAnchor:

  To anchor using the subset cohort's start date or end date. The
  parameter is specified as 'cohortStart' or 'cohortEnd'.

- negate:

  Allows for negating a window, a way to detect that a subset does not
  occur relative to a target

## Value

a SubsetCohortWindow instance
