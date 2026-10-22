library(testthat)
library(CohortGenerator)

testPlatform <- function(context) {
  cohortTableNames <- getCohortTableNames(cohortTable = context$objectPrefix)
  platformOutputFolder <- file.path(outputFolder, context$dbms)
  on.exit({
    dropCohortStatsTables(
      connectionDetails = context$connectionDetails,
      cohortDatabaseSchema = context$workDatabaseSchema,
      cohortTableNames = cohortTableNames,
      dropCohortTable = TRUE
    )
    unlink(platformOutputFolder, recursive = TRUE)
  }, add = TRUE)

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
    createCohortSubsetOperator(
      cohortIds = 2,
      cohortCombinationOperator = "all",
      negate = FALSE,
      windows = list(
        createSubsetCohortWindow(
          startDay = -99999,
          endDay = 99999,
          targetAnchor = "cohortStart",
          subsetAnchor = "cohortStart"
        ),
        createSubsetCohortWindow(
          startDay = -99999,
          endDay = 99999,
          targetAnchor = "cohortEnd",
          subsetAnchor = "cohortEnd"
        )
      )
    ),
    createLimitSubsetOperator(
      priorTime = 365,
      followUpTime = 0,
      calendarStartDate = lubridate::date("2001/1/1"),
      calendarEndDate = lubridate::date("2019/1/31"),
      limitTo = "earliestRemaining"
    ),
    createDemographicSubsetOperator(
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
    connectionDetails = context$connectionDetails,
    cdmDatabaseSchema = context$cdmDatabaseSchema,
    cohortDatabaseSchema = context$workDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithSubsets,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE,
    outputFolder = platformOutputFolder,
    databaseId = context$dbms,
    incremental = FALSE
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
  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")
  Panoptes::skipIfNoLiveDatabase(config)
  dbms <- Panoptes::getSelectedTestDbms(config)

  Panoptes::withDatabaseTestContext(
    config = config,
    dbms = dbms,
    legacyAliases = NULL,
    verify = TRUE,
    {
      message("Testing ", context$dbms)
      testPlatform(context)
      Panoptes::requireDatabaseTestConnectionAttempted(context)
    }
  )
})
