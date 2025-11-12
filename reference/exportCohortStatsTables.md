# Export the cohort statistics tables to the file system

This function retrieves the data from the cohort statistics tables and
writes them to the inclusion statistics folder specified in the function
call. NOTE: inclusion rule names are handled in one of two ways:

1\. You can specify the cohortDefinitionSet parameter and the inclusion
rule names will be extracted from the data.frame. 2. You can insert the
inclusion rule names into the database using the
insertInclusionRuleNames function of this package.

The first approach is preferred as to avoid the warning emitted.

## Usage

``` r
exportCohortStatsTables(
  connectionDetails,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortStatisticsFolder,
  snakeCaseToCamelCase = TRUE,
  fileNamesInSnakeCase = FALSE,
  incremental = FALSE,
  databaseId = NULL,
  minCellCount = 5,
  cohortDefinitionSet = NULL,
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

- cohortStatisticsFolder:

  The path to the folder where the cohort statistics folder where the
  results will be written

- snakeCaseToCamelCase:

  Should column names in the exported files convert from snake_case to
  camelCase? Default is FALSE

- fileNamesInSnakeCase:

  Should the exported files use snake_case? Default is FALSE

- incremental:

  If `incremental = TRUE`, results are written to update values instead
  of overwriting an existing results (deprecated)

- databaseId:

  Optional - when specified, the databaseId will be added to the
  exported results

- minCellCount:

  To preserve privacy: the minimum number of subjects contributing to a
  count before it can be included in the results. If the count is below
  this threshold, it will be set to \`-minCellCount\`.

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

- tablePrefix:

  Optional - allows to append a prefix to the exported file names.
