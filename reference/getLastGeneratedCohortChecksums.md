# Get last generated cohort checksums

This gets a log of the last checksum for each cohort id stored in the
cohort_checksum table.

This should be used to audit cohort generation as (if generated with
cohort_generator) cohorts should always have an end time in this table.
The last end time will be the cohort that is in the cohort table
(assuming no other manual modifications are made to the cohort table
itself).

This can be used downstream of CohortGenerator to evaluate if cohorts
are consistent with passed definitions.

## Usage

``` r
getLastGeneratedCohortChecksums(
  connectionDetails = NULL,
  connection = NULL,
  cohortId = NULL,
  cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  .checkTables = TRUE
)
```

## Arguments

- connectionDetails:

  An object of type `connectionDetails` as created using the
  [`createConnectionDetails`](https://ohdsi.github.io/DatabaseConnector/reference/createConnectionDetails.html)
  function in the DatabaseConnector package. Can be left NULL if
  `connection` is provided.

- connection:

  An object of type `connection` as created using the
  [`connect`](https://ohdsi.github.io/DatabaseConnector/reference/connect.html)
  function in the DatabaseConnector package. Can be left NULL if
  `connectionDetails` is provided, in which case a new connection will
  be opened at the start of the function, and closed when the function
  finishes.

- cohortId:

  cohortId to check. If NULL, all cohorts will be returned.

- cohortDatabaseSchema:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- cohortTableNames:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

- .checkTables:

  used internally
