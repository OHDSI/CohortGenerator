# CohortGenerator

[![Build Status](https://github.com/OHDSI/CohortGenerator/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortGenerator/actions?query=workflow%3AR-CMD-check) [![codecov.io](https://codecov.io/github/OHDSI/CohortGenerator/coverage.svg?branch=main)](https://codecov.io/github/OHDSI/CohortGenerator?branch=main)

CohortGenerator is part of [HADES](https://ohdsi.github.io/Hades/).

# Introduction

This R package contains functions for generating cohorts using data in the CDM.

# Features

-   Create a cohort table and generate [cohorts](https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html) against an OMOP CDM.
-   Get the count of subjects and events in a cohort.
-   Provides functions for performing incremental tasks. This is used by CohortGenerator to skip any cohorts that were successfully generated in a previous run. This functionality is generic enough for other packages to use for performing their own incremental tasks.

# Example

``` r
# First construct a cohort definition set: an empty 
# data frame with the cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this 
# package as an example
cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  # Here we read in the JSON in order to create the SQL
  # using [CirceR](https://ohdsi.github.io/CirceR/)
  # If you have your JSON and SQL stored differenly, you can
  # modify this to read your JSON/SQL files however you require
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = i,
                                                       cohortName = cohortName, 
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))
}

# Generate the cohort set against Eunomia. 
# cohortsGenerated contains a list of the cohortIds 
# successfully generated against the CDM
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "my_cohort_table")
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                                        cohortDatabaseSchema = "main",
                                                        cohortTableNames = cohortTableNames)
# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = "main",
                                                       cohortDatabaseSchema = "main",
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = cohortsToCreate)

# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                 cohortDatabaseSchema = "main",
                                                 cohortTable = cohortTableNames$cohortTable)
print(cohortCounts)
```

# Technology

CohortGenerator is an R package.

# System requirements

Requires R (version 3.6.0 or higher).

# Getting Started

1.  Make sure your R environment is properly configured. This means that Java must be installed. See [these instructions](https://ohdsi.github.io/Hades/rSetup.html) for how to configure your R environment.

2.  In R, use the following commands to download and install CohortGenerator:

    ``` r
    remotes::install_github("OHDSI/CohortGenerator")
    ```

# User Documentation

Documentation can be found on the [package website](https://ohdsi.github.io/CohortGenerator/).

PDF versions of the documentation are also available:

-   Vignette: [Generating Cohorts](https://raw.githubusercontent.com/OHDSI/CohortGenerator/main/inst/doc/GeneratingCohorts.pdf)
-   Package manual: [CohortGenerator.pdf](https://raw.githubusercontent.com/OHDSI/CohortGenerator/main/extras/CohortGenerator.pdf)

# Support

-   Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
-   We use the <a href="https://github.com/OHDSI/CohortGenerator/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

# Contributing

Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

# License

CohortGenerator is licensed under Apache License 2.0

# Development

This package is being developed in RStudio.

### Development status

Beta
