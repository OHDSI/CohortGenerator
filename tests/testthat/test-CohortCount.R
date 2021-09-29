library(testthat)
library(CohortGenerator)

# Test Prep ----------------
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cohortCounts <- Eunomia::createCohorts(connectionDetails = connectionDetails,
                                       cdmDatabaseSchema = "main",
                                       cohortDatabaseSchema = "main",
                                       cohortTable = "cohort")

test_that("Call getCohortCounts without connection or connectionDetails", {
  expect_error(getCohortCounts(),
               message = "(connection details)")
})

test_that("Call getCohortCounts with cohort table", {
  connection <- DatabaseConnector::connect(connectionDetails)
  testCohortCounts <- getCohortCounts(connection = connection,
                                      cohortDatabaseSchema = "main",
                                      cohortTable = "cohort")
  matchedCohortCounts <- merge(testCohortCounts, cohortCounts)
  expect_true(nrow(matchedCohortCounts[matchedCohortCounts$cohortSubjects == matchedCohortCounts$count,]) == nrow(cohortCounts))
  on.exit(DatabaseConnector::disconnect(connection))
})

test_that("Call getCohortCounts with cohort table that does not exist", {
  expect_warning(getCohortCounts(connectionDetails = connectionDetails,
                                      cohortDatabaseSchema = "main",
                                      cohortTable = "foobar"),
                 message = "(Cohort table was not found)")
})

test_that("Call getCohortCounts with subset of cohort IDs", {
  testCohortCounts <- getCohortCounts(connectionDetails = connectionDetails,
                                      cohortDatabaseSchema = "main",
                                      cohortTable = "cohort",
                                      cohortIds = c(1,2))
  matchedCohortCounts <- merge(testCohortCounts, cohortCounts)
  expect_true(nrow(matchedCohortCounts[matchedCohortCounts$cohortSubjects == matchedCohortCounts$count,]) == nrow(testCohortCounts))
})
