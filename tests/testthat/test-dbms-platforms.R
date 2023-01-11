testPlatform <- function(dbmsDetails) {
  cohortTableNames <- getCohortTableNames(cohortTable = dbmsDetails$cohortTable)
  on.exit({
    dropCohortStatsTables(connectionDetails = dbmsDetails$connectionDetails,
                          cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
                          cohortTableNames = cohortTableNames,
                          dropCohortTable = TRUE)
  })

  createCohortTables(
    connectionDetails = dbmsDetails$connectionDetails,
    cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames
  )
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

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