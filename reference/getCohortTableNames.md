# Used to get a list of cohort table names to use when creating the cohort tables

This function creates a list of table names used by
[`createCohortTables`](https://ohdsi.github.io/CohortGenerator/reference/createCohortTables.md)
to specify the table names to create. Use this function to specify the
names of the main cohort table and cohort statistics tables.

## Usage

``` r
getCohortTableNames(
  cohortTable = "cohort",
  cohortSampleTable = cohortTable,
  cohortInclusionTable = paste0(cohortTable, "_inclusion"),
  cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
  cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
  cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"),
  cohortCensorStatsTable = paste0(cohortTable, "_censor_stats"),
  cohortChecksumTable = paste0(cohortTable, "_checksum")
)
```

## Arguments

- cohortTable:

  Name of the cohort table.

- cohortSampleTable:

  Name of the cohort table for sampled cohorts (defaults to the same as
  the cohort table).

- cohortInclusionTable:

  Name of the inclusion table, one of the tables for storing inclusion
  rule statistics.

- cohortInclusionResultTable:

  Name of the inclusion result table, one of the tables for storing
  inclusion rule statistics.

- cohortInclusionStatsTable:

  Name of the inclusion stats table, one of the tables for storing
  inclusion rule statistics.

- cohortSummaryStatsTable:

  Name of the summary stats table, one of the tables for storing
  inclusion rule statistics.

- cohortCensorStatsTable:

  Name of the censor stats table, one of the tables for storing
  inclusion rule statistics.

- cohortChecksumTable:

  Stores the checksum of the cohort used and the time generation starts
  and ends

## Value

A list of the table names as specified in the parameters to this
function.
