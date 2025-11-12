# Upload results to the database server.

Requires the results data model tables have been created using the
[`createResultsDataModel`](https://ohdsi.github.io/CohortGenerator/reference/createResultsDataModel.md)
function.

## Usage

``` r
uploadResults(
  connectionDetails,
  schema,
  resultsFolder,
  forceOverWriteOfSpecifications = FALSE,
  purgeSiteDataBeforeUploading = TRUE,
  tablePrefix = "",
  ...
)
```

## Arguments

- connectionDetails:

  An object of type `connectionDetails` as created using the
  [`createConnectionDetails`](https://ohdsi.github.io/DatabaseConnector/reference/createConnectionDetails.html)
  function in the DatabaseConnector package.

- schema:

  The schema on the server where the tables have been created.

- resultsFolder:

  The folder holding the results in .csv files

- forceOverWriteOfSpecifications:

  If TRUE, specifications of the phenotypes, cohort definitions, and
  analysis will be overwritten if they already exist on the database.
  Only use this if these specifications have changed since the last
  upload.

- purgeSiteDataBeforeUploading:

  If TRUE, before inserting data for a specific databaseId all the data
  for that site will be dropped. This assumes the resultsFolder file
  contains the full data for that data site.

- tablePrefix:

  (Optional) string to insert before table names for database table
  names

- ...:

  See ResultModelManager::uploadResults
