# Export cohort subset statistics tables to the file system

This function retrieves the data from the cohort subset statistics table
and writes them to the subset statistics folder specified in the
function call.

## Usage

``` r
exportCohortSubsetStatsTables(
  connectionDetails,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortSubsetStatisticsFolder,
  snakeCaseToCamelCase = TRUE,
  fileNamesInSnakeCase = FALSE,
  databaseId = NULL,
  minCellCount = 5,
  tablePrefix = ""
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

- cohortTableNames:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

- cohortSubsetStatisticsFolder:

  The path to the folder where the cohort subset statistics results will
  be written.

- snakeCaseToCamelCase:

  Should column names in the exported files convert from snake_case to
  camelCase? Default is FALSE

- fileNamesInSnakeCase:

  Should the exported files use snake_case? Default is FALSE

- databaseId:

  Optional - when specified, the databaseId will be added to the
  exported results

- minCellCount:

  To preserve privacy: the minimum number of subjects contributing to a
  count before it can be included in the results. If the count is below
  this threshold, it will be set to \`-minCellCount\`.

- tablePrefix:

  Optional - allows to append a prefix to the exported file names.
