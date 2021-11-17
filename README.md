# CohortGenerator

[![Build Status](https://github.com/OHDSI/CohortGenerator/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortGenerator/actions?query=workflow%3AR-CMD-check) [![codecov.io](https://codecov.io/github/OHDSI/CohortGenerator/coverage.svg?branch=master)](https://codecov.io/github/OHDSI/CohortGenerator?branch=master)

CohortGenerator is part of [HADES](https://ohdsi.github.io/Hades/).

# Introduction

This R package contains functions for generating cohorts using data in the CDM.

# Features

-   Create a cohort table and generate [cohorts](https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html) against an OMOP CDM.
-   Get the count of subjects and events in a cohort.
-   Provides functions for performing incremental tasks. This is used by CohortGenerator to skip any cohorts that were successfully generated in a previous run. This functionality is generic enough for other packages to use for performing their own incremental tasks.

# Example

``` r
# First construct cohort set: an empty data frame with the
# cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortSet()

# Fill the cohort set using  cohorts included in this 
# package as an example
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  # Here we read in the JSON in order to create the SQL
  # using [CirceR](https://ohdsi.github.io/CirceR/)
  # If you have your JSON and SQL stored differenly, you can
  # modify this to read your JSON/SQL files however you require
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = i,
                                                       cohortFullName = cohortFullName, 
                                                       sql = cohortSql,
                                                       json = cohortJson,
                                                       stringsAsFactors = FALSE))
}

# Generate the cohort set against Eunomia. 
# cohortsGenerated contains a list of the cohortIds 
# successfully generated against the CDM
outputFolder <- "C:/TEMP"
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = "main",
                                                       cohortDatabaseSchema = "main",
                                                       cohortTable = "temp_cohort",
                                                       cohortSet = cohortsToCreate,
                                                       createCohortTable = TRUE,
                                                       incremental = FALSE,
                                                       incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                                       inclusionStatisticsFolder = outputFolder)
                                                       
# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                 cohortDatabaseSchema = "main",
                                                 cohortTable = "temp_cohort")
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

-   Package manual: [CohortGenerator.pdf](https://raw.githubusercontent.com/OHDSI/CohortGenerator/master/extras/CohortGenerator.pdf)

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
