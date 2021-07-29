library(testthat)
library(CohortGenerator)

# Test Prep ----------------
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# Helper Functions
generateSql <- function(cohortJsonFileName, generateStats = FALSE) {
  cohortExpression <- CohortGenerator::createCirceExpressionFromFile(cohortJsonFileName)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = generateStats))
  return(cohortSql)
}

# Used to add a SQL column to the "cohorts" data frame
# and toggle if inclusion stats are generated for the given SQL
# definition
getCohortsForTest <- function(cohorts, generateStats = FALSE) {
  cohortSql <- data.frame()
  for (i in 1:nrow(cohorts)) {
    cohortSql <- rbind(cohortSql, data.frame(sql = generateSql(cohorts$cohortJsonFile[i], generateStats)))
  }
  if (length(intersect(colnames(cohorts), c("sql"))) == 1) {
    cohorts$sql <- NULL
  }
  cohorts <- cbind(cohorts, cohortSql)
  return(cohorts)
}

# This will gather all of the cohort JSON in the package for use in the tests
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
cohorts <- setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "json", "cohortJsonFile"))
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- CohortGenerator::readCirceExpressionJsonFile(cohortJsonFileName)
  cohortExpression <- CohortGenerator::createCirceExpressionFromFile(cohortJsonFileName)
  cohorts <- rbind(cohorts, data.frame(cohortId = i, 
                                       cohortFullName = cohortFullName, 
                                       json = cohortJson,
                                       cohortJsonFile = cohortJsonFileName,
                                       stringsAsFactors = FALSE))
}

# Exception Handling -------------
# instantiateCohortSet ---------
test_that("Call instantiateCohortSet without connection or connectionDetails", {
  expect_error(instantiateCohortSet(),
               message = "(connection details)")
})

test_that("Call instantiateCohortSet with default parameters", {
  expect_error(instantiateCohortSet(connectionDetails = c()),
               message = "(cohorts parameter)")
})

test_that("Call instatiateCohortSet with malformed cohort parameter", {
  expect_error(instantiateCohortSet(connectionDetails = connectionDetails,
                                    cohortSet = data.frame()),
               message = "(must contain the following columns)")
})

test_that("Call instatiateCohortSet with vector as cohort parameter", {
  expect_error(instantiateCohortSet(connectionDetails = connectionDetails,
                                    cohortSet = c()),
               message = "(data frame)")
})

test_that("Call instatiateCohortSet with incremental = TRUE and no folder specified", {
  expect_error(instantiateCohortSet(connectionDetails = connectionDetails,
                                    cohortSet = createEmptyCohortSet(),
                                    incremental = TRUE),
               message = "Must specify incrementalFolder")
})

test_that("Ensure instatiateCohortSet is single threaded", {
  expect_error(instantiateCohortSet(connectionDetails = connectionDetails,
                                    cohortSet = createEmptyCohortSet(),
                                    numThreads = 2),
               message = "numThreads must be set to 1 for now.")
})


# createCohortTable ---------
test_that("Call createCohortTable without connection or connectionDetails", {
  expect_error(createCohortTable(),
               message = "(connection details)")
})

# getInclusionStatistics ------
test_that("Call getInclusionStatistics without connection or connectionDetails", {
  expect_error(getInclusionStatistics(),
               message = "(connection details)")
})

# Functional Tests ----------------
test_that("Create cohort table with connection", {
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  createCohortTable(connection = conn,
                    cohortTable = "test_cohort_table",
                    cohortDatabaseSchema = "main")

  results <- DatabaseConnector::querySql(conn, sql = "SELECT * FROM test_cohort_table;")
  expect_equal(nrow(results), 0)
  DatabaseConnector::disconnect(conn)
})

test_that("Create cohort table with connectionDetails", {
  createCohortTable(connectionDetails = connectionDetails,
                    cohortTable = "test_cohort_table",
                    cohortDatabaseSchema = "main")
  
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::querySql(conn, sql = "SELECT * FROM test_cohort_table;")
  expect_equal(nrow(results), 0)
  DatabaseConnector::disconnect(conn)
})

test_that("Create cohort table and inclusion stats table with connection", {
  conn = DatabaseConnector::connect(connectionDetails = connectionDetails)
  tableList <- data.frame(cohortTable = "test_cohort_table",
                          cohortInclusionTable = "test_cohort_inc",
                          cohortInclusionResultTable = "test_cohort_inc_result",
                          cohortInclusionStatsTable = "test_cohort_inc_stats",
                          cohortSummaryStatsTable = "test_cohort_summary",
                          cohortCensorStatsTable = "test_cohort_censor_stats")
  createCohortTable(connection = conn,
                    cohortTable = tableList$cohortTable,
                    cohortDatabaseSchema = "main",
                    createInclusionStatsTables = TRUE,
                    cohortInclusionTable = tableList$cohortInclusionTable,
                    cohortInclusionResultTable = tableList$cohortInclusionResultTable,
                    cohortInclusionStatsTable = tableList$cohortInclusionStatsTable,
                    cohortSummaryStatsTable = tableList$cohortSummaryStatsTable,
                    cohortCensorStatsTable = tableList$cohortCensorStatsTable)
  
  for(i in colnames(tableList)) {
    sql <- paste("SELECT * FROM", tableList[[i]], ";")
    results <- DatabaseConnector::querySql(conn, sql = sql)
    expect_equal(nrow(results), 0)
  }
  DatabaseConnector::disconnect(conn)
})

test_that("Create cohorts - Gen Stats = T, Incremental = F, Gather Results", {
  outputFolder <- tempdir()

  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithStats,
                                           createCohortTable = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohortsWithStats))
  rm(cohortsWithStats)
  unlink(outputFolder)
})

test_that("Create cohorts - Gen Stats = T, Incremental = T", {
  outputFolder <- tempdir()
  # Run first to ensure that all cohorts are generated
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithStats,
                                           createCohortTable = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  # Next run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithStats,
                                           createCohortTable = TRUE,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohortsWithStats))
  rm(cohortsWithStats)
  unlink(outputFolder)
})

test_that("Create cohorts - Gen Stats = F, Incremental = F", {
  outputFolder <- tempdir()
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithoutStats,
                                           createCohortTable = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohortsWithoutStats))
  rm(cohortsWithoutStats)
  unlink(outputFolder)
})

test_that("Create cohorts - Gen Stats = F, Incremental = T", {
  outputFolder <- tempdir()
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithoutStats,
                                           createCohortTable = TRUE,
                                           incremental = FALSE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  # Next run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- instantiateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTable = "temp_cohort",
                                           cohortSet = cohortsWithoutStats,
                                           createCohortTable = TRUE,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"),
                                           inclusionStatisticsFolder = outputFolder)
  expect_equal(length(cohortsGenerated), nrow(cohortsWithoutStats))
  unlink(outputFolder)
})

# Cleanup ------
rm(generateSql)
rm(getCohortsForTest)
rm(cohortJsonFiles)
rm(cohorts)
rm(connectionDetails) # Remove the Eunomia database
