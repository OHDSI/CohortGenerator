# Sample Cohort Definition Set

Create 1 or more sample of size n of a cohort definition set

Subsetted cohorts can be sampled, as with any other subset form.
However, subsetting a sampled cohort is not recommended and not
currently supported at this time. In the case where n \> cohort count
the entire cohort is copied unmodified

As different databases have different forms of randomness, the random
selection is computed in R, based on the count for each cohort. This is,
therefore, db platform independent

Note, this function assumes cohorts have already been generated.

Lifecycle Note: This functionality is considered experimental and not
intended for use inside analytic packages

## Usage

``` r
sampleCohortDefinitionSet(
  cohortDefinitionSet,
  cohortIds = cohortDefinitionSet$cohortId,
  connectionDetails = NULL,
  connection = NULL,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema,
  outputDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  n = NULL,
  sampleFraction = NULL,
  seed = 64374,
  seedArgs = NULL,
  identifierExpression = "cohortId * 1000 + seed",
  incremental = FALSE,
  incrementalFolder = NULL
)
```

## Arguments

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

- cohortIds:

  Optional subset of cohortIds to generate. By default this function
  will sample all cohorts

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

- tempEmulationSchema:

  Some database platforms like Oracle and Impala do not truly support
  temp tables. To emulate temp tables, provide a schema with write
  privileges where temp tables can be created.

- cohortDatabaseSchema:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- outputDatabaseSchema:

  optional schema to output cohorts to (if different from
  cohortDatabaseSchema)

- cohortTableNames:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

- n:

  Sample size. Ignored if sample fraction is set

- sampleFraction:

  Fraction of cohort to sample

- seed:

  Vector of seeds to give to the R pseudorandom number generator

- seedArgs:

  optional arguments to pass to set.seed

- identifierExpression:

  Optional string R expression used to compute output cohort id. Can
  only use variables cohortId and seed. Default is "cohortId \* 1000 +
  seed", which is substituted and evaluated

- incremental:

  Create only cohorts that haven't been created before?

- incrementalFolder:

  If `incremental = TRUE`, specify a folder where records are kept of
  which definition has been executed. (deprceated)

## Value

sampledCohortDefinitionSet - a data.frame like object that contains the
resulting identifiers and modified names of cohorts
