# Create SNOMED Cohort Template Definition

Template cohort definition for all OHDSI standard conditions. The
cohortId = conceptId \* 1000. The "identifierExpression" can be
customized for uniqueness. This definition uses any valid SNOMED
condition code and all its descendants.

Excluded terms include word patterns:

' '

Cohorts are first event.

## Usage

``` r
createSnomedCohortTemplateDefinition(
  connection,
  identifierExpression = "CAST(concept_id as bigint) * 1000",
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  priorObservationPeriod = 365,
  requireSecondDiagnosis = FALSE,
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

- priorObservationPeriod:

  (optional) Required prior observation period for individuals

- requireSecondDiagnosis:

  (optional) Require more than one diagnosis code

- nameSuffix:

  A name suffix to use to add to the cohort names - this is useful if
  you're using multiple parameterized versions of this definition

- vocabularyDatabaseSchema:

  Vocabulary database schema

## Value

A CohortTemplateDefinition instance
