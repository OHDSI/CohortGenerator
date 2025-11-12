# Get a cohort definition set

This function supports the legacy way of retrieving a cohort definition
set from the file system or in a package. This function supports the
legacy way of storing a cohort definition set in a package with a CSV
file, JSON files, and SQL files in the \`inst\` folder.

## Usage

``` r
getCohortDefinitionSet(
  settingsFileName = "Cohorts.csv",
  jsonFolder = "cohorts",
  sqlFolder = "sql/sql_server",
  cohortFileNameFormat = "%s",
  cohortFileNameValue = c("cohortId"),
  subsetJsonFolder = "inst/cohort_subset_definitions/",
  templateFolder = "inst/cohort_template_definitions/",
  packageName = NULL,
  warnOnMissingJson = TRUE,
  verbose = FALSE
)
```

## Arguments

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

  Defines the folder to store sql template cohorts that can be loaded as
  part of the definition JSON files are loaded into cohort definition
  set

- packageName:

  The name of the package containing the cohort definitions.

- warnOnMissingJson:

  Provide a warning if a .JSON file is not found for a cohort in the
  settings file

- verbose:

  When TRUE, extra logging messages are emitted

## Value

Returns a cohort set data.frame
