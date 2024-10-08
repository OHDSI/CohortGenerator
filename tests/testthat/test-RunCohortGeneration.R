library(testthat)
library(CohortGenerator)

# Exception Handling -------------
test_that("Call runCohortGeneration without connectionDetails", {
  expect_error(runCohortGeneration(), message = "(connection details)")
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
  diffs <- setdiff(expectedFileList, basename(list.files(testOutputFolder)))
  expect_true(length(diffs) == 0)

  # Make sure that each output file contains the same columns as defined
  # in the specification
  for (i in seq_along(expectedFileList)) {
    data <- readCsv(
      file = file.path(testOutputFolder, expectedFileList[i])
    )
    tbl <- tools::file_path_sans_ext(expectedFileList[i])

    emptyResult <- CohortGenerator:::createEmptyResult(tbl)
    expect_equal(!!c(tbl, sort(names(data))), !!c(tbl, sort(names(emptyResult))))
  }

  # Make sure that the output that specifies a database ID has the correct
  # value included
  tablesWithDatabaseId <- spec %>%
    dplyr::filter(columnName == "database_id")
  for (i in seq_along(tablesWithDatabaseId)) {
    # Read in the data and ensure all of the database_ids match the
    # the one used in the test
    data <- CohortGenerator::readCsv(
      file = file.path(testOutputFolder, paste0(tablesWithDatabaseId$tableName[i], ".csv"))
    )
    expect_true(all(data$databaseId == expectedDatabaseId))
  }
})

test_that("Call runCohortGeneration and verify censoring of minimum cell counts", {
  testOutputFolder1 <- file.path(outputFolder, "minCellCountRun1")
  testOutputFolder2 <- file.path(outputFolder, "minCellCountRun2")
  on.exit(expr = {
    unlink(testOutputFolder1, recursive = TRUE)
    unlink(testOutputFolder2, recursive = TRUE)
  })
  minCellCountForTest <- 500
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
    outputFolder = testOutputFolder1,
    databaseId = expectedDatabaseId,
    minCellCount = 0,
    incremental = F
  )

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
    outputFolder = testOutputFolder2,
    databaseId = expectedDatabaseId,
    minCellCount = minCellCountForTest,
    incremental = F
  )


  # Verify that values below the minCellCountForTest are censored properly
  spec <- CohortGenerator::readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(tolower(.data$minCellCount) == "yes") %>%
    dplyr::arrange(.data$tableName, .data$columnName)

  for (i in 1:nrow(spec)) {
    data1 <- readr::read_csv(file = file.path(testOutputFolder1, paste0(spec$tableName[i], ".csv")), col_types = readr::cols(), lazy = F)
    data2 <- readr::read_csv(file = file.path(testOutputFolder2, paste0(spec$tableName[i], ".csv")), col_types = readr::cols(), lazy = F)
    rowsBelowMinCellCount <- which(data1[[spec$columnName[i]]] < minCellCountForTest)
    expect_true(all(data2[rowsBelowMinCellCount, spec$columnName[i]] == -minCellCountForTest))
  }
})
