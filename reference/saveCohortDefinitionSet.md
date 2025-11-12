# Save the cohort definition set to the file system

This function saves a cohortDefinitionSet to the file system and
provides options for specifying where to write the individual elements:
the settings file will contain the cohort information as a CSV specified
by the settingsFileName, the cohort JSON is written to the jsonFolder
and the SQL is written to the sqlFolder. We also provide a way to
specify the json/sql file name format using the cohortFileNameFormat and
cohortFileNameValue parameters.

## Usage

``` r
saveCohortDefinitionSet(
  cohortDefinitionSet,
  settingsFileName = "inst/Cohorts.csv",
  jsonFolder = "inst/cohorts",
  sqlFolder = "inst/sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/cohort_subset_definitions/",
  templateFolder = "inst/cohort_template_definitions/",
  verbose = FALSE
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

- settingsFileName:

  The name of the CSV file that will hold the cohort information
  including the cohortId and cohortName

- jsonFolder:

  The name of the folder that will hold the JSON representation of the
  cohort if it is available in the cohortDefinitionSet

- sqlFolder:

  The name of the folder that will hold the SQL representation of the
  cohort.

- cohortFileNameFormat:

  Defines the format string for naming the cohort JSON and SQL files.
  The format string follows the standard defined in the base sprintf
  function.

- cohortFileNameValue:

  Defines the columns in the cohortDefinitionSet to use in conjunction
  with the cohortFileNameFormat parameter.

- subsetJsonFolder:

  Defines the folder to store the subset JSON

- templateFolder:

  Defines the folder to store sql template cohorts that can be saved as
  part of the definition Sql will be copied to this location when
  \`saveCohortDefinitionSet\` is called.

- verbose:

  When TRUE, logging messages are emitted to indicate export progress.
