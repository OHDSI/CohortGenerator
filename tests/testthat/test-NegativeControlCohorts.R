library(testthat)
library(CohortGenerator)

# createEmptyNegativeControlOutcomeCohortSet ----------
test_that("Call createEmptyNegativeControlOutcomeCohortSet in verbose mode", {
  expect_output(createEmptyNegativeControlOutcomeCohortSet(verbose = TRUE))
})


# generateNegativeControlOutcomeCohorts -------
test_that("Call generateNegativeControlOutcomeCohorts without connection or connectionDetails", {
  expect_error(generateNegativeControlOutcomeCohorts())
})

test_that("Call generateNegativeControlOutcomeCohorts before creating cohort table fails", {
  cohortTableNames <- getCohortTableNames(cohortTable = "missing_cohort_table")
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expect_error(
    generateNegativeControlOutcomeCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "all",
      detectOnDescendants = TRUE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with occurrenceType == 'all' and detectOnDescendants == FALSE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "ot_all_dod_f")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "all",
      detectOnDescendants = FALSE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with occurrenceType == 'first' and detectOnDescendants == FALSE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "ot_first_dod_f")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "first",
      detectOnDescendants = FALSE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with occurrenceType == 'all' and detectOnDescendants == TRUE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "ot_all_dod_t")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "all",
      detectOnDescendants = TRUE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with occurrenceType == 'first' and detectOnDescendants == TRUE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "ot_first_dod_t")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest()
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "first",
      detectOnDescendants = TRUE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with custom cohort ids", {
  cohortTableNames <- getCohortTableNames(cohortTable = "nc_custom_cohortid")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest(setCohortIdToConceptId = FALSE)
  generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE
  )
  cohortCounts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = cohortTableNames$cohortTable
  )
  expect_equal(cohortCounts$cohortId, ncSet$cohortId)
})
