testPlatform <- function(dbmsDetails) {
  cohortTableNames <- getCohortTableNames(cohortTable = dbmsDetails$cohortTable)
  on.exit({
    dropCohortStatsTables(
      connectionDetails = dbmsDetails$connectionDetails,
      cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = TRUE
    )
  })

  createCohortTables(
    connectionDetails = dbmsDetails$connectionDetails,
    cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames
  )
  cohortsWithStats <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )

  cohortsGenerated <- generateCohortSet(
    connectionDetails = dbmsDetails$connectionDetails,
    cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
    cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats,
    incremental = TRUE,
    incrementalFolder = file.path(outputFolder, "RecordKeeping", dbmsDetails$connectionDetails$dbms)
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithStats))

  subsetOperations <- list(
    createCohortSubset(
      cohortIds = 2,
      cohortCombinationOperator = "all",
      negate = FALSE,
      startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
      endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
    ),
    createLimitSubset(
      priorTime = 365,
      followUpTime = 0,
      calendarStartDate = lubridate::date("2001/1/1"),
      calendarEndDate = lubridate::date("2019/1/31"),
      limitTo = "earliestRemaining"
    ),
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )
  cohortsWithSubsets <- addCohortSubsetDefinition(cohortsWithStats, subsetDef)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = dbmsDetails$connectionDetails,
    cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
    cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithSubsets,
    incremental = TRUE,
    incrementalFolder = file.path(outputFolder, "RecordKeeping", dbmsDetails$connectionDetails$dbms)
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithSubsets))
}

# This file contains platform specific tests
test_that("platform specific create cohorts with stats, Incremental, get results", {
  # Note that these tests are designed to be quick and just test the platform in a general way
  # Sqlite completes the bulk of the packages testing
  for (dbmsPlatform in dbmsPlatforms) {
    dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)
    if (is.null(dbmsDetails)) {
      print(paste("No pltatform details available for", dbmsPlatform))
    } else {
      testPlatform(dbmsDetails)
    }
  }
})
