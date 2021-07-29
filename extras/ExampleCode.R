# First construct a data frame with the cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortSet()

# Use the cohorts included in this package as an example
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"),
                              full.names = TRUE)
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- CohortGenerator::readCirceExpressionJsonFile(cohortJsonFileName)
  cohortExpression <- CohortGenerator::createCirceExpressionFromFile(cohortJsonFileName)
  cohortsToCreate <- rbind(cohortsToCreate,
                           data.frame(cohortId = i, cohortFullName = cohortFullName,
    sql = CirceR::buildCohortQuery(cohortExpression,
                                   options = CirceR::createGenerateOptions(generateStats = FALSE)),
    json = cohortJson, stringsAsFactors = FALSE))
}

# Instantiate the cohort set against Eunomia. cohortGenerated contains a list of the cohortIds
# generated against CDM
outputFolder <- "C:/TEMP"
cohortsGenerated <- instantiateCohortSet(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
  cdmDatabaseSchema = "main", cohortDatabaseSchema = "main", cohortTable = "temp_cohort", cohortSet = cohortsToCreate,
  createCohortTable = TRUE, incremental = TRUE, incrementalFolder = file.path(outputFolder,
                                                                              "RecordKeeping"),
  inclusionStatisticsFolder = outputFolder)

