# Get Cohort Inclusion Stats Table Data

This function returns a data frame of the data in the Cohort Inclusion
Tables. Results are organized in to a list with 6 different data frames:

- cohortInclusionTable

- cohortInclusionResultTable

- cohortInclusionStatsTable

- cohortSummaryStatsTable

- cohortCensorStatsTable

- cohortAttritionTable

These can be optionally specified with the `outputTables`. See
`exportCohortStatsTables` function for saving data to csv.

## Usage

``` r
getCohortStats(
  connectionDetails,
  connection = NULL,
  cohortDatabaseSchema,
  databaseId = NULL,
  snakeCaseToCamelCase = TRUE,
  outputTables = c("cohortInclusionTable", "cohortInclusionResultTable",
    "cohortInclusionStatsTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable",
    "cohortCensorStatsTable", "cohortAttritionTable"),
  cohortTableNames = getCohortTableNames(),
  inclusionRules = NULL
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

- cohortDatabaseSchema:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- databaseId:

  Optional - when specified, the databaseId will be added to the
  exported results

- snakeCaseToCamelCase:

  Convert column names from snake case to camel case.

- outputTables:

  Character vector. One or more of "cohortInclusionTable",
  "cohortInclusionResultTable", "cohortInclusionStatsTable",
  "cohortInclusionStatsTable", "cohortSummaryStatsTable" or
  "cohortCensorStatsTable", "cohortAttritionTable". Output is limited to
  these tables. Cannot export, for, example, the cohort table. Defaults
  to all stats tables.

- cohortTableNames:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

- inclusionRules:

  A data.frame with inclusion rules from the cohortDefinitionSet used to
  generate the cohort stats obtained by running
  `getCohortInclusionRules(cohortDefinitionSet)` (Optional)
