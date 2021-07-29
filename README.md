# CohortGenerator

[![Build Status](https://github.com/OHDSI/CohortGenerator/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CohortGenerator/actions?query=workflow%3AR-CMD-check) [![codecov.io](https://codecov.io/github/OHDSI/CohortGenerator/coverage.svg?branch=master)](https://codecov.io/github/OHDSI/CohortGenerator?branch=master)

CohortGenerator is part of [HADES](https://ohdsi.github.io/Hades/).

# Introduction

This R package contains functions for instantiating cohorts using data in the CDM.

# Features

-   Instantiates cohorts cohorts against a CDM.
-   Provides functions for generating SQL from [CirceR](https://github.com/OHDSI/CirceR) compliant JSON definitions.
-   Provides functions for performing incremental tasks. This is used by CohortGenerator to skip any cohorts that were successfully instantiated in a previous run. This functionality is generic enough for other packages to use for performing their own incremental tasks.

# Example

``` r
# First construct a data frame with the cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortSet()

# Use the cohorts included in this package as an example
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- CohortGenerator::readCirceExpressionJsonFile(cohortJsonFileName)
  cohortExpression <- CohortGenerator::createCirceExpressionFromFile(cohortJsonFileName)
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = i,
                                                       cohortFullName = cohortFullName, 
                                                       sql = CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE)),
                                                       json = cohortJson,
                                                       stringsAsFactors = FALSE))
}

# Instantiate the cohort set against Eunomia. 
# cohortGenerated contains a list of the cohortIds 
# successfully generated against the CDM
outputFolder <- "C:/TEMP"
cohortsGenerated <- instantiateCohortSet(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
                                         cdmDatabaseSchema = "main",
                                         cohortDatabaseSchema = "main",
                                         cohortTable = "temp_cohort",
                                         cohortSet = cohortsToCreate,
                                         createCohortTable = TRUE,
                                         incremental = TRUE,
                                         incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                         inclusionStatisticsFolder = outputFolder)
```

# Technology

This an R package with some dependencies requiring Java.

# System requirements

Requires R and Java.

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
