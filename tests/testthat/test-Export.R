library(testthat)
library(CohortGenerator)

# export cohort stats tests --------------
test_that("Export cohort stats with permanent tables", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsPerm")
  cohortStatsFolder <- file.path(outputFolder, "stats")
  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Test getting results data frames
  cohortStats <- getCohortStats(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    snakeCaseToCamelCase = FALSE,
    databaseId = "Eunomia"
  )

  checkmate::expect_names(names(cohortStats),
    must.include = c(
      "cohortInclusionTable",
      "cohortInclusionResultTable",
      "cohortInclusionStatsTable",
      "cohortInclusionStatsTable",
      "cohortSummaryStatsTable",
      "cohortCensorStatsTable"
    )
  )

  for (tbl in names(cohortStats)) {
    checkmate::expect_data_frame(cohortStats[[tbl]])
  }

  # Test bad table name
  expect_error(
    cohortStats <- getCohortStats(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = "main",
      cohortTableNames = cohortTableNames,
      outputTables = c("cohort"),
      databaseId = "Eunomia"
    )
  )

  # Test only exporting single table
  cohortStats <- getCohortStats(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    outputTables = c("cohortInclusionStatsTable"),
    databaseId = "Eunomia"
  )

  checkmate::expect_names(names(cohortStats), subset.of = c("cohortInclusionStatsTable"))
  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    incremental = FALSE
  )

  # Verify the files are written to the file system
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = "*.csv")
  expect_equal(length(exportedFiles), 5)
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats with databaseId", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsDatabaseId")
  cohortStatsFolder <- file.path(outputFolder, "stats")
  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Generate with stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  generateCohortSet(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cdmDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = "main",
    incremental = FALSE
  )

  # Insert the inclusion rules
  insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  )

  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    incremental = FALSE,
    databaseId = "Eunomia"
  )

  # Verify the files are written to the file system and have the database_id
  # present
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  for (i in 1:length(exportedFiles)) {
    data <- CohortGenerator:::.readCsv(file = exportedFiles[i])
    if (basename(exportedFiles[i]) == "cohortInclusion.csv") {
      expect_false(toupper(c("databaseId")) %in% toupper(names(data)))
    } else {
      expect_true(toupper(c("databaseId")) %in% toupper(names(data)))
    }
  }
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats with fileNamesInSnakeCase = TRUE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsSnakeCase")
  cohortStatsFolder <- file.path(outputFolder, "snakeCaseStats")
  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Generate with stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  generateCohortSet(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cdmDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = "main",
    incremental = FALSE
  )

  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    fileNamesInSnakeCase = TRUE,
    incremental = FALSE,
    databaseId = "Eunomia"
  )

  # Verify the files are written to the file system and are in snake_case
  # present
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv")
  for (i in 1:length(exportedFiles)) {
    expect_true(isSnakeCase(tools::file_path_sans_ext(exportedFiles[i])))
  }
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats in incremental mode", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsPerm")
  cohortStatsFolder <- file.path(outputFolder, "stats")
  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    incremental = TRUE
  )

  # Verify the files are written to the file system
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  expect_equal(length(exportedFiles), 5)
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats with camelCase for column names", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsSnakeCase")
  cohortStatsFolder <- file.path(outputFolder, "statsCamelCase")

  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Generate the cohorts
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats
  )

  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    snakeCaseToCamelCase = TRUE,
    fileNamesInSnakeCase = TRUE,
    incremental = TRUE
  )

  # Verify the files are written to the file system and the columns are in
  # camel case format
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  for (i in 1:length(exportedFiles)) {
    data <- CohortGenerator:::.readCsv(exportedFiles[i])
    expect_true(all(isCamelCase(names(data))))
  }

  # Export the results again in incremental mode and verify
  # the results are preserved
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    snakeCaseToCamelCase = TRUE,
    fileNamesInSnakeCase = TRUE,
    incremental = TRUE
  )

  # Verify the cohort_inc_stats.csv contains cohortDefinitionIds c(2,3)
  # camel case format
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = "cohort_inc_stats.csv", full.names = TRUE)
  expect_equal(length(exportedFiles), 1)
  data <- CohortGenerator:::.readCsv(exportedFiles[1])
  expect_equal(unique(data$cohortDefinitionId), c(2, 3))
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats with snake_case for column names", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStats_snake")
  cohortStatsFolder <- file.path(outputFolder, "statsSnakeCase")

  # First create the cohort tables
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Generate the cohorts
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats
  )

  # Export the results
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    snakeCaseToCamelCase = FALSE,
    fileNamesInSnakeCase = TRUE,
    incremental = TRUE
  )

  # Verify the files are written to the file system and the columns are in
  # camel case format
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  for (i in 1:length(exportedFiles)) {
    data <- CohortGenerator:::.readCsv(exportedFiles[i])
    expect_true(all(isSnakeCase(names(data))))
  }

  # Export the results again in incremental mode and verify
  # the results are preserved
  exportCohortStatsTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = cohortStatsFolder,
    snakeCaseToCamelCase = FALSE,
    fileNamesInSnakeCase = TRUE,
    incremental = TRUE
  )

  # Verify the cohort_inc_stats.csv contains cohort_definition_id == c(2,3)
  # snake case format
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = "cohort_inc_stats.csv", full.names = TRUE)
  expect_equal(length(exportedFiles), 1)
  data <- CohortGenerator:::.readCsv(exportedFiles[1])
  expect_equal(unique(data$cohort_definition_id), c(2, 3))
  unlink(cohortStatsFolder)
})
