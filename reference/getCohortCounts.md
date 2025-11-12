# Count the cohort(s)

Computes the subject and entry count per cohort. Note the
cohortDefinitionSet parameter is optional - if you specify the
cohortDefinitionSet, the cohort counts will be joined to the
cohortDefinitionSet to include attributes like the cohortName.

## Usage

``` r
getCohortCounts(
  connectionDetails = NULL,
  connection = NULL,
  cohortDatabaseSchema,
  cohortTable = "cohort",
  cohortIds = c(),
  cohortDefinitionSet = NULL,
  databaseId = NULL
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

  Schema name where your cohort table resides. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- cohortTable:

  The name of the cohort table.

- cohortIds:

  The cohort Id(s) used to reference the cohort in the cohort table. If
  left empty and no \`cohortDefinitionSet\` argument is specified, all
  cohorts in the table will be included. If you specify the
  \`cohortIds\` AND \`cohortDefinitionSet\`, the counts will reflect the
  \`cohortIds\` from the \`cohortDefinitionSet\`.

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

- databaseId:

  Optional - when specified, the databaseId will be added to the
  exported results

## Value

A data frame with cohort counts
