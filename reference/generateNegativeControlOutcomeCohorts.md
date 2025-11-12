# Generate a set of negative control outcome cohorts

This function generate a set of negative control outcome cohorts. For
more information please see \[Chapter 12 - Population Level
Estimation\](https://ohdsi.github.io/TheBookOfOhdsi/PopulationLevelEstimation.html)
for more information how these cohorts are utilized in a study design.

## Usage

``` r
generateNegativeControlOutcomeCohorts(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortTable = cohortTableNames$cohortTable,
  negativeControlOutcomeCohortSet,
  occurrenceType = "all",
  incremental = FALSE,
  incrementalFolder = NULL,
  detectOnDescendants = FALSE
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

- cohortTable:

  Name of the cohort table.

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

  The occurrenceType will detect either: the first time an
  outcomeConceptId occurs or all times the outcomeConceptId occurs for a
  person. Values accepted: 'all' or 'first'.

- incremental:

  Create only cohorts that haven't been created before?

- incrementalFolder:

  If `incremental = TRUE`, specify a folder where records are kept of
  which definition has been executed. (deprecated)

- detectOnDescendants:

  When set to TRUE, detectOnDescendants will use the vocabulary to find
  negative control outcomes using the outcomeConceptId and all
  descendants via the concept_ancestor table. When FALSE, only the exact
  outcomeConceptId will be used to detect the outcome.

## Value

Invisibly returns an empty negative control outcome cohort set
data.frame
