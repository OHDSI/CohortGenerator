library(testthat)
library(CohortGenerator)

# getCohortTableNames ---------
test_that("Call getCohortTableNames with defaults", {
  expect_equal(getCohortTableNames(),
               list(cohortTable = "cohort",
                    cohortInclusionTable = "cohort_inclusion",
                    cohortInclusionResultTable = "cohort_inclusion_result",
                    cohortInclusionStatsTable = "cohort_inclusion_stats",
                    cohortSummaryStatsTable = "cohort_summary_stats",
                    cohortCensorStatsTable = "cohort_censor_stats"))
})

test_that("Call getCohortTableNames with custom table names", {
  expect_equal(getCohortTableNames(cohortTable = "a",
                                   cohortInclusionTable = "b",
                                   cohortInclusionResultTable = "c",
                                   cohortInclusionStatsTable = "d",
                                   cohortSummaryStatsTable = "e",
                                   cohortCensorStatsTable = "f"),
               list(cohortTable = "a",
                    cohortInclusionTable = "b",
                    cohortInclusionResultTable = "c",
                    cohortInclusionStatsTable = "d",
                    cohortSummaryStatsTable = "e",
                    cohortCensorStatsTable = "f"))
})

# createCohortTables ---------
test_that("Call createCohortTables without connection or connectionDetails", {
  expect_error(createCohortTables(),
               message = "(connection details)")
})

test_that("Create cohort tables with connectionDetails", {
  expect_message(
    createCohortTables(connectionDetails = connectionDetails,
                       cohortDatabaseSchema = "main")
  )
})

test_that("Create cohort tables with connection", {
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  cohortTableNames <- getCohortTableNames(cohortTable = "a",
                                          cohortInclusionTable = "b",
                                          cohortInclusionResultTable = "c",
                                          cohortInclusionStatsTable = "d",
                                          cohortSummaryStatsTable = "e",
                                          cohortCensorStatsTable = "f")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)

  for(i in names(cohortTableNames)) {
    sql <- paste("SELECT * FROM", cohortTableNames[[i]], ";")
    results <- DatabaseConnector::querySql(conn, sql = sql)
    expect_equal(nrow(results), 0)
  }
  DatabaseConnector::disconnect(conn)
})

test_that("Create cohort tables with incremental = TRUE", {
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  cohortTableNames <- getCohortTableNames(cohortTable = "incrementalTrue")
  # Call the 1st time and verify the tables exist
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames,
                     incremental = TRUE)

  for(i in names(cohortTableNames)) {
    sql <- paste("SELECT * FROM", cohortTableNames[[i]], ";")
    results <- DatabaseConnector::querySql(conn, sql = sql)
    expect_equal(nrow(results), 0)
  }

  # Call again and verify the table creation is skipped
  expect_invisible(
    createCohortTables(connectionDetails = connectionDetails,
                       cohortDatabaseSchema = "main",
                       cohortTableNames = cohortTableNames,
                       incremental = TRUE)
  )

  DatabaseConnector::disconnect(conn)
})

test_that("Create cohort tables with incremental = TRUE and partial table creation works", {
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  cohortTableNames <- getCohortTableNames(cohortTable = "incrementalPartial")

  # Create only a cohort table
  sql <- "IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
          DROP TABLE @cohort_database_schema.@cohort_table;

          CREATE TABLE @cohort_database_schema.@cohort_table (
            cohort_definition_id BIGINT,
            subject_id BIGINT,
            cohort_start_date DATE,
            cohort_end_date DATE
          );

          INSERT INTO @cohort_database_schema.@cohort_table (
            cohort_definition_id,
            subject_id,
            cohort_start_date,
            cohort_end_date
          )
          SELECT
            1,1,1.0,1.0
          ;
  "
  sql <- SqlRender::render(sql = sql, cohort_database_schema = "main", cohort_table = cohortTableNames$cohortTable)
  sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
  DatabaseConnector::executeSql(connection = conn, sql = sql, progressBar = FALSE, reportOverallTime = FALSE)

  # Verify the table exists and contains a record
  sql <- paste0("SELECT * FROM main.", cohortTableNames$cohortTable, ";")
  results <- DatabaseConnector::querySql(conn, sql = sql)
  expect_equal(nrow(results), 1)

  # Create the cohort tables and verify
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames,
                     incremental = TRUE)

  for(i in names(cohortTableNames)) {
    sql <- paste("SELECT * FROM", cohortTableNames[[i]], ";")
    results <- DatabaseConnector::querySql(conn, sql = sql)
    expectedRowCount <- ifelse(cohortTableNames[[i]] == cohortTableNames$cohortTable, 1, 0)
    expect_equal(nrow(results), expectedRowCount)
  }

  DatabaseConnector::disconnect(conn)
})

# export cohort stats tests --------------
test_that("Export cohort stats with permanent tables", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsPerm")
  cohortStatsFolder <- tempdir()
  # First create the cohort tables
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)

  # Test getting results data frames
  cohortStats <- getCohortStats(connectionDetails = connectionDetails,
                                cohortDatabaseSchema = "main",
                                cohortTableNames = cohortTableNames,
                                databaseId = "Eunomia")

  checkmate::expect_names(names(cohortStats),
                         must.include = c("cohortInclusionTable",
                                          "cohortInclusionResultTable",
                                          "cohortInclusionStatsTable",
                                          "cohortInclusionStatsTable",
                                          "cohortSummaryStatsTable",
                                          "cohortCensorStatsTable"))

  for (tbl in names(cohortStats)) {
    checkmate::expect_data_frame(cohortStats[[tbl]])
  }

  # Test bad table name
  expect_error(
    cohortStats <- getCohortStats(connectionDetails = connectionDetails,
                                  cohortDatabaseSchema = "main",
                                  cohortTableNames = cohortTableNames,
                                  outputTables = c("cohort"),
                                  databaseId = "Eunomia")
  )

  # Test only exporting single table
  cohortStats <- getCohortStats(connectionDetails = connectionDetails,
                                  cohortDatabaseSchema = "main",
                                  cohortTableNames = cohortTableNames,
                                  outputTables = c("cohortInclusionStatsTable"),
                                  databaseId = "Eunomia")

  checkmate::expect_names(names(cohortStats), subset.of = c("cohortInclusionStatsTable"))
  # Export the results
  exportCohortStatsTables(connectionDetails = connectionDetails,
                          cohortDatabaseSchema = "main",
                          cohortTableNames = cohortTableNames,
                          cohortStatisticsFolder = cohortStatsFolder,
                          incremental = FALSE)

  # Verify the files are written to the file system
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = "*.csv")
  expect_equal(length(exportedFiles), 5)
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats with databaseId", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsDatabaseId")
  cohortStatsFolder <- tempdir()
  # First create the cohort tables
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  
  # Generate with stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  generateCohortSet(connectionDetails = connectionDetails,
                    cohortDefinitionSet = cohortsWithStats,
                    cdmDatabaseSchema = "main",
                    cohortTableNames = cohortTableNames,
                    cohortDatabaseSchema = "main",
                    incremental = FALSE)

  # Export the results
  exportCohortStatsTables(connectionDetails = connectionDetails,
                          cohortDatabaseSchema = "main",
                          cohortTableNames = cohortTableNames,
                          cohortStatisticsFolder = cohortStatsFolder,
                          incremental = FALSE,
                          databaseId = "Eunomia")
  
  # Verify the files are written to the file system and have the database_id
  # present
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  for (i in 1:length(exportedFiles)) {
    data <- readCsv(file = exportedFiles[i])
    expect_true(toupper(c("database_id")) %in% toupper(names(data)))
  }
  unlink(cohortStatsFolder)
})

test_that("Export cohort stats in incremental mode", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsPerm")
  cohortStatsFolder <- tempdir()
  # First create the cohort tables
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)

  # Export the results
  exportCohortStatsTables(connectionDetails = connectionDetails,
                          cohortDatabaseSchema = "main",
                          cohortTableNames = cohortTableNames,
                          cohortStatisticsFolder = cohortStatsFolder,
                          incremental = TRUE)

  # Verify the files are written to the file system
  exportedFiles <- list.files(path = cohortStatsFolder, pattern = ".csv", full.names = TRUE)
  expect_equal(length(exportedFiles), 5)
  unlink(cohortStatsFolder)
})

# drop cohort stats tables --------------
test_that("Drop cohort stats tables", {
  cohortTableNames <- getCohortTableNames(cohortTable = "cohortStatsDropTest")
  # First create the cohort tables
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  
  # Drop the cohort stats tables
  dropCohortStatsTables(connectionDetails = connectionDetails,
                        cohortDatabaseSchema = "main",
                        cohortTableNames = cohortTableNames)

  # Verify that the only table remaining is the main cohort table
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  tables <- DatabaseConnector::getTableNames(connection = connection,
                                             databaseSchema = "main")

  expect_true(tolower(cohortTableNames$cohortTable) %in% tolower(tables))
  expect_false(tolower(cohortTableNames$cohortInclusionTable) %in% tolower(tables))
  expect_false(tolower(cohortTableNames$cohortInclusionResultTable) %in% tolower(tables))
  expect_false(tolower(cohortTableNames$cohortInclusionStatsTable) %in% tolower(tables))
  expect_false(tolower(cohortTableNames$cohortSummaryStatsTable) %in% tolower(tables))
  expect_false(tolower(cohortTableNames$cohortCensorStatsTable) %in% tolower(tables))
})
