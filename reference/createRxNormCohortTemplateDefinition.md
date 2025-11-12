# Create Rx Norm Cohort Template Definition

Template cohort definition for all RxNorm ingredients. This cohort will
use the vocabulary tables to automatically generate a set of cohorts
that have the cohortId = conceptId \* 1000. The "identifierExpression"
can be customized for uniqueness.

## Usage

``` r
createRxNormCohortTemplateDefinition(
  connection,
  identifierExpression = "CAST(concept_id as bigint) * 1000",
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema,
  priorObservationPeriod = 365,
  nameSuffix = "",
  vocabularyDatabaseSchema = cdmDatabaseSchema
)
```

## Arguments

- connection:

  Database connection object

- identifierExpression:

  An expression for setting the cohort id for the resulting cohort. Must
  produce unique ids

- cdmDatabaseSchema:

  CDM database schema

- tempEmulationSchema:

  Temporary emulation schema

- cohortDatabaseSchema:

  Cohort database schema

- priorObservationPeriod:

  (optional) Required prior observation period for individuals

- nameSuffix:

  A name suffix to use to add to the cohort names - this is useful if
  you're using multiple parameterized versions of this definition

- vocabularyDatabaseSchema:

  Vocabulary database schema

## Value

A CohortTemplateDefinition instance
