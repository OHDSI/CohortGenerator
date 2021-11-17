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

test_that("Create cohort table with connection", {
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
  DatabaseConnector::disconnect(conn)})
