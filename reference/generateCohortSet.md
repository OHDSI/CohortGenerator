# Generate a set of cohorts

This function generates a set of cohorts in the cohort table.

## Usage

``` r
generateCohortSet(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortDefinitionSet = NULL,
  stopOnError = TRUE,
  incremental = FALSE,
  incrementalFolder = NULL
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

- cdmDatabaseSchema:

  Schema name where your patient-level data in OMOP CDM format resides.
  Note that for SQL Server, this should include both the database and
  schema name, for example 'cdm_data.dbo'.

- tempEmulationSchema:

  Some database platforms like Oracle and Impala do not truly support
  temp tables. To emulate temp tables, provide a schema with write
  privileges where temp tables can be created.

- cohortDatabaseSchema:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- cohortTableNames:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

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

- stopOnError:

  If an error happens while generating one of the cohorts in the
  cohortDefinitionSet, should we stop processing the other cohorts? The
  default is TRUE; when set to FALSE, failures will be identified in the
  return value from this function.

- incremental:

  Create only cohorts that haven't been created before?

- incrementalFolder:

  If `incremental = TRUE`, specify a folder where records are kept of
  which definition has been executed. (deprceated)

## Value

A data.frame consisting of the following columns:

- cohortId:

  The unique integer identifier of the cohort

- cohortName:

  The cohort's name

- generationStatus:

  The status of the generation task which may be one of the following:

  COMPLETE

  :   The generation completed successfully

  FAILED

  :   The generation failed (see logs for details)

  SKIPPED

  :   If using incremental == 'TRUE', this status indicates that the
      cohort's generation was skipped since it was previously completed.

- startTime:

  The start time of the cohort generation. If the generationStatus ==
  'SKIPPED', the startTime will be NA.

- endTime:

  The end time of the cohort generation. If the generationStatus ==
  'FAILED', the endTime will be the time of the failure. If the
  generationStatus == 'SKIPPED', endTime will be NA.
