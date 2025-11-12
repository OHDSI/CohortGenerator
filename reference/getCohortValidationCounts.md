# Validate cohort

Using custom sql, it is possible to generate cohorts that are not
technically definitions. Invalid cohorts include the following:

\* Cohorts where individuals have multiple, overlapping eras \* Cohorts
that have start dates that occur after their end dates \* Cohorts with
duplicate entries for the same subject.

Additionally the count for cohorts that lie outside the observation
period for individuals is added. However, due to valid reasons in cohort
definitions (e.g. fixed cohort duration, data source context) this
cannot be directly considered a pass/fail diagnostic in all contexts.

Note - this code cannot formally verify the validity of a cohort. There
may be situations where the logic of a cohort definition only causes
errors in certain circumstances. Furthermore, if cohort counts are 0
this check is unable to evaluate validity at all.

The returned data.frame counts the number of errors found for each
cohort. In addition a boolean "valid" field is applied that is TRUE only
in the case where all counts are 0.

## Usage

``` r
getCohortValidationCounts(
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = getCohortTableNames(),
  cohortIds = NULL
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

- cohortIds:

  Ids of cohorts to validate

## Value

a data.frame with the fields cohortId, overlappingErasCount,
invalidDateCount, duplicateCount, outsideObservationCount
