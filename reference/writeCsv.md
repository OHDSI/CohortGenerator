# Used to write a .csv file

This function is used to centralize the function for writing .csv files
across the HADES ecosystem. This function will automatically convert
from camelCase in the data.frame to snake_case column names in the
resulting .csv file as is the standard described in:
https://ohdsi.github.io/Hades/codeStyle.html#Interfacing_between_R_and_SQL

This function may also raise warnings if the data is stored in a format
that will not work with the HADES standard for uploading to a results
database. Specifically file names should be in snake_case format, all
column headings are in snake_case format and where possible the file
name should not be plural. See `isFormattedForDatabaseUpload` for a
helper function to check a data.frame for rules on the column names

## Usage

``` r
writeCsv(
  x,
  file,
  append = FALSE,
  warnOnCaseMismatch = TRUE,
  warnOnFileNameCaseMismatch = TRUE,
  warnOnUploadRuleViolations = TRUE
)
```

## Arguments

- x:

  A data frame or tibble to write to disk.

- file:

  The .csv file to write.

- append:

  When TRUE, append the values of x to an existing file.

- warnOnCaseMismatch:

  When TRUE, raise a warning if columns in the data.frame are NOT in
  camelCase format.

- warnOnFileNameCaseMismatch:

  When TRUE, raise a warning if the file name specified is not in
  snake_case format.

- warnOnUploadRuleViolations:

  When TRUE, this function will provide warning messages that may
  indicate if the data is stored in a format in the .csv that may cause
  problems when uploading to a database.

## Value

Returns the input x invisibly.
