# Create ATC Cohort Template Definition

Template cohort definition for all ATC level 4 class exposures. The
cohortId = conceptId \* 1000 + 4. The "identifierExpression" can be
customized for uniqueness.

## Usage

``` r
createAtcCohortTemplateDefinition(
  connection,
  identifierExpression = "CAST(concept_id as bigint) * 1000",
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema,
  nameSuffix = "",
  mergeIngredientEras = TRUE,
  priorObservationPeriod = 365,
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

- nameSuffix:

  A name suffix to use to add to the cohort names - this is useful if
  you're using multiple parameterized versions of this definition

- mergeIngredientEras:

  (optional) Boolean indicating if different ingredients under the same
  ATC code should be merged

- priorObservationPeriod:

  (optional) Required prior observation period for individuals

- vocabularyDatabaseSchema:

  Vocabulary database schema

## Value

A CohortTemplateDefinition instance
