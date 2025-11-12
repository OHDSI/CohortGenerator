# Run a cohort generation and export results

Run a cohort generation and export results

## Usage

``` r
runCohortGeneration(
  connectionDetails,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortDefinitionSet = NULL,
  negativeControlOutcomeCohortSet = NULL,
  occurrenceType = "all",
  detectOnDescendants = FALSE,
  stopOnError = TRUE,
  outputFolder,
  databaseId = 1,
  minCellCount = 5,
  incremental = FALSE,
  incrementalFolder = NULL
)
```

## Arguments

- connectionDetails:

  An object of type `connectionDetails` as created using the
  [`createConnectionDetails`](https://ohdsi.github.io/DatabaseConnector/reference/createConnectionDetails.html)
  function in the DatabaseConnector package.

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

- negativeControlOutcomeCohortSet:

  The `negativeControlOutcomeCohortSet` argument must be a data frame
  with the following columns:

  cohortId

  :   The unique integer identifier of the cohort

  cohortName

  :   The cohort's name

  outcomeConceptId

  :   The concept_id in the condition domain to use for the negative
      control outcome.

- occurrenceType:

  For negative controls outcomes, the occurrenceType will detect either:
  the first time an outcomeConceptId occurs or all times the
  outcomeConceptId occurs for a person. Values accepted: 'all' or
  'first'.

- detectOnDescendants:

  For negative controls outcomes, when set to TRUE, detectOnDescendants
  will use the vocabulary to find negative control outcomes using the
  outcomeConceptId and all descendants via the concept_ancestor table.
  When FALSE, only the exact outcomeConceptId will be used to detect the
  outcome.

- stopOnError:

  If an error happens while generating one of the cohorts in the
  cohortDefinitionSet, should we stop processing the other cohorts? The
  default is TRUE; when set to FALSE, failures will be identified in the
  return value from this function.

- outputFolder:

  Name of the folder where all the outputs will written to.

- databaseId:

  A unique ID for the database. This will be appended to most tables.

- minCellCount:

  To preserve privacy: the minimum number of subjects contributing to a
  count before it can be included in the results. If the count is below
  this threshold, it will be set to \`-minCellCount\`.

- incremental:

  Create only cohorts that haven't been created before?

- incrementalFolder:

  If `incremental = TRUE`, specify a folder where records are kept of
  which definition has been executed. (deprecated)

## Details

Run a cohort generation for a set of cohorts and negative control
outcomes. This function will also export the results of the run to the
\`outputFolder\`.
