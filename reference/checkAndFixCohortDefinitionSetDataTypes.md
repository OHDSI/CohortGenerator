# Check if a cohort definition set is using the proper data types

This function checks a data.frame to verify it holds the expected format
for a cohortDefinitionSet's data types and can optionally fix data types
that do not match the specification.

## Usage

``` r
checkAndFixCohortDefinitionSetDataTypes(
  x,
  fixDataTypes = TRUE,
  emitWarning = FALSE
)
```

## Arguments

- x:

  The cohortDefinitionSet data.frame to check

- fixDataTypes:

  When TRUE, this function will attempt to fix the data types to match
  the specification. @seealso \[createEmptyCohortDefinitionSet()\].

- emitWarning:

  When TRUE, this function will emit warning messages when problems are
  encountered.

## Value

Returns a list() of the following form:

list( dataTypesMatch = TRUE/FALSE, x = data.frame() )

dataTypesMatch == TRUE when the supplied data.frame x matches the
cohortDefinitionSet specification's data types.

If fixDataTypes == TRUE, x will hold the original data from x with the
data types corrected. Otherwise x will hold the original value passed to
this function.
