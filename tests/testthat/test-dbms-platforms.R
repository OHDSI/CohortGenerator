library(testthat)
library(CohortGenerator)

testPlatform <- function(dbmsDetails) {
  cohortTableNames <- getCohortTableNames(cohortTable = dbmsDetails$cohortTable)
  platformOutputFolder <- file.path(outputFolder, dbmsDetails$connectionDetails$dbms)
  on.exit({
    dropCohortStatsTables(
      connectionDetails = dbmsDetails$connectionDetails,
      cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = TRUE
    )
    unlink(platformOutputFolder, recursive = TRUE)
  })

  cohortsWithStats <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
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

  ncSet <- getNegativeControlOutcomeCohortsForTest()

  runCohortGeneration(
    connectionDetails = dbmsDetails$connectionDetails,
    cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
    cohortDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithSubsets,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE,
    outputFolder = platformOutputFolder,
    databaseId = dbmsDetails$connectionDetails$dbms,
    incremental = F
  )

  # Check the output to verify the generation worked properly
  cohortsGenerated <- readCsv(
    file = file.path(platformOutputFolder, "cg_cohort_generation.csv")
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithSubsets))

  cohortCounts <- readCsv(
    file = file.path(platformOutputFolder, "cg_cohort_count.csv")
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortCounts))

  ncCohortCounts <- readCsv(
    file = file.path(platformOutputFolder, "cg_cohort_count_neg_ctrl.csv")
  )
  expect_equal(nrow(ncSet), nrow(ncCohortCounts))
}

# This file contains platform specific tests
test_that("platform specific create cohorts with stats, Incremental, get results", {
  skip_on_cran()
  # Note that these tests are designed to be quick and just test the platform in a general way
  # Sqlite completes the bulk of the packages testing
  for (dbmsPlatform in dbmsPlatforms) {
    dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)
    if (is.null(dbmsDetails)) {
      print(paste("No platform details available for", dbmsPlatform))
    } else {
      print(paste("Testing", dbmsPlatform))
      testPlatform(dbmsDetails)
    }
  }
})
