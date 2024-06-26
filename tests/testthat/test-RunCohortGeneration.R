library(testthat)
library(CohortGenerator)

# Exception Handling -------------
test_that("Call runCohortGeneration without connectionDetails", {
  expect_error(runCohortGeneration(), message = "(connection details)"
  )
})

test_that("Call runCohortGeneration without connectionDetails", {
  expect_error(
    runCohortGeneration(
      connectionDetails = connectionDetails
    ), 
    message = "(You must supply at least 1 cohortDefinitionSet OR 1 negativeControlOutcomeCohortSet)"
  )
})

test_that("Call runCohortGeneration happy path", {
  testOutputFolder <- file.path(outputFolder, "runCG")
  on.exit(unlink(testOutputFolder, recursive = TRUE))
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expectedDatabaseId <- "db1"
  
  runCohortGeneration(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = CohortGenerator::getCohortTableNames("runCG"),
    cohortDefinitionSet = cohortsWithStats,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "all",
    detectOnDescendants = TRUE,
    stopOnError = FALSE,
    outputFolder = testOutputFolder,
    databaseId = expectedDatabaseId,
    incremental = F
  )
  
  # Ensure the resultsDataModelSpecification.csv is written
  # to the output folder
  expect_true(file.exists(file.path(testOutputFolder, "resultsDataModelSpecification.csv")))

  # Make sure the output includes a file for every table in the spec
  spec <- CohortGenerator::readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  )
  expectedFileList <- paste0(unique(spec$tableName), ".csv")
  diffs <- setdiff(basename(list.files(testOutputFolder)), expectedFileList)
  
  # NOTE: The only difference between the specification and the
  # output folder is expected to be the resultsDataModelSpecification.csv
  expect_true(all(diffs == "resultsDataModelSpecification.csv"))
  
  # Make sure that the output that specifies a database ID has the correct
  # value included
  tablesWithDatabaseId <- spec %>%
    dplyr::filter(columnName == 'database_id')
  for (i in seq_along(tablesWithDatabaseId)) {
    # Read in the data and ensure all of the database_ids match the
    # the one used in the test
    data <- CohortGenerator::readCsv(
      file = file.path(testOutputFolder, paste0(tablesWithDatabaseId$tableName[i], ".csv"))
    )
    expect_true(all(data$databaseId == expectedDatabaseId))
  }
})