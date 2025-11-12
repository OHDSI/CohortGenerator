# Create Cohort Template Definition

construct a cohort template definition

## Usage

``` r
createCohortTemplateDefintion(
  name,
  templateSql,
  references,
  sqlArgs = list(),
  translateSql = TRUE
)
```

## Arguments

- name:

  A name for the template definition. This is not used in the checksum
  of the cohort

- templateSql:

  Sql string that is used to generate the cohorts. This should be in
  OHDSI sql form, translatable to other db platforms.

- references:

  This is a data frame that must contain cohortId and cohortName.
  Optionally, this can contain the columns sql and json as well. It must
  be bindable to a cohort definition set instance.

- sqlArgs:

  Optional parameters for execution of the query - for example
  vocabulary schema These are arguments that should be passed to the
  sql. These are used in the checksum if using parameterized sql for
  different definitions (e.g. a definition requiring varying observation
  lengths. This is used to distinguish them) This should not include
  cdm/data source specific parameters such as the cohort table names,
  cdm database schema or vocabulary database schema. If the definition
  requires runtime specific arguments (e.g. non standard tables) this
  presents a problem for serializing and uniquely identifying template
  cohort definitions.

- translateSql:

  to translate the sql or not.
