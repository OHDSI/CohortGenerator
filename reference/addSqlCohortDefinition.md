# Add an sql cohort definition

This is useful in cases where it is difficult or impossible to define a
cohort in Circe. This utility should be used sparingly, but is
convenient non-the-less. Note that no checks on this definition occur
and, in principle, any sql can be executed. Incremental execution and
logging will work. This should also be compatible with other OHDSI
packages that use standard cohort tables.

All cohorts should result in standard cohort tables which have the
columns:

\* cohort_definition_id, \* subject_id, \* cohort_start_date, \*
cohort_end_date

As these are requirements of cohorts.

The sql parameters: cohort_table, cohort_database_schema,
cdm_database_schema and vocabulary_database_schema should not be
specified in the arguments to this function. These cohorts can be
serialized with saveCohortDefinitionSet and shared so should not include
data source specific content.

## Usage

``` r
addSqlCohortDefinition(
  cohortDefinitionSet,
  sql,
  cohortId,
  cohortName,
  tanslateSql = TRUE,
  json = NULL,
  ...
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

- sql:

  OHDSI SqlRender-compatible sql

- cohortId:

  Id of cohort to add. Must be unique in the cohort definition set

- cohortName:

  Name of the cohort to add

- tanslateSql:

  perform translation on the sql. This is ignored if the sql has already
  been translated with the sql render function.

- json:

  optional json parameters

- ...:

  arguments for the sql. Note that this does not need to include
  cohort_table, cohort_database_schema, cdm_database_schema or
  vocabulary_database_schema

## Examples

``` r

sql <- "INSERT INTO @cohort_database_schema.@cohort_table
             (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
        SELECT 1 as cohort_definition_id,
               person_id as subject_id,
               drug_era_start_date as cohort_start_date,
               drug_era_end_data as cohort_end_date
        FROM @cdm_database_schema.drug_era de
        INNER JOIN @vocabulary_database_schema.concept c on de.drug_concept_id = c.concept_id
        -- Find any matches of drugs named 'asprin' in the drug concept table
        WHERE lower(c.concept_name) like '%asprin%'; "

cohortDefinitionSet <- createEmptyCohortDefinitionSet() |>
  addSqlCohortDefinition(sql = sql, cohortId = 1, cohortName = "my asprin cohort")
```
