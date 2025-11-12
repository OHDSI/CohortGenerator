# Is the data.frame formatted for uploading to a database?

This function is used to check a data.frame to ensure all column names
are in snake case format.

## Usage

``` r
isFormattedForDatabaseUpload(x, warn = TRUE)
```

## Arguments

- x:

  A data frame

- warn:

  When TRUE, display a warning of any columns are not in snake case
  format

## Value

Returns TRUE if all columns are snake case format. If warn == TRUE, the
function will emit a warning on the column names that are not in snake
case format.
