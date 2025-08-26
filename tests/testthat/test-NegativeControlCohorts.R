library(testthat)
library(dplyr)
library(CohortGenerator)

# createEmptyNegativeControlOutcomeCohortSet ----------
test_that("Call createEmptyNegativeControlOutcomeCohortSet in verbose mode", {
  expect_output(createEmptyNegativeControlOutcomeCohortSet(verbose = TRUE))
})


# generateNegativeControlOutcomeCohorts -------
test_that("Call generateNegativeControlOutcomeCohorts without connection or connectionDetails", {
  expect_error(generateNegativeControlOutcomeCohorts())
})

test_that("Call generateNegativeControlOutcomeCohorts with negativeControlOutcomeCohortSet containing non-integer cohort ID", {
  negativeControlOutcomeCohortSet <- data.frame(
    cohortId = 1.2,
    cohortName = "invalid cohort id",
    outcomeConceptId = 1
  )
  expect_error(
    generateNegativeControlOutcomeCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet
    ),
    message = "(non-integer values)"
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with negativeControlOutcomeCohortSet containing non-integer outcome concept ID", {
  negativeControlOutcomeCohortSet <- data.frame(
    cohortId = 1,
    cohortName = "invalid outcome concept id",
    outcomeConceptId = 1.2
  )
  expect_error(
    generateNegativeControlOutcomeCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet
    ),
    message = "(non-integer values)"
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with negativeControlOutcomeCohortSet containing duplicate IDs", {
  negativeControlOutcomeCohortSet <- data.frame(
    cohortId = 1,
    cohortName = "duplicate #1",
    outcomeConceptId = 1
  )
  negativeControlOutcomeCohortSet <- rbind(
    negativeControlOutcomeCohortSet,
    data.frame(
      cohortId = 1,
      cohortName = "duplicate #2",
      outcomeConceptId = 1
    )
  )
  expect_error(
    generateNegativeControlOutcomeCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet
    ),
    message = "(Cannot generate! Duplicate cohort IDs found in your negativeControlOutcomeCohortSet)"
  )
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
      cohortTableNames = cohortTableNames,
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
      cohortTableNames = cohortTableNames,
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
      cohortTableNames = cohortTableNames,
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
      cohortTableNames = cohortTableNames,
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
      cohortTableNames = cohortTableNames,
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
    cohortTableNames = cohortTableNames,
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
      cohortTableNames = cohortTableNames,
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "first",
      detectOnDescendants = FALSE
    )
  )
})

test_that("Call generateNegativeControlOutcomeCohorts with occurrenceType == 'first' and detectOnDescendants == FALSE multiple times to ensure there are no duplicates", {
  cohortTableNames <- getCohortTableNames(cohortTable = "ot_first_dod_f")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest()

  # Set the cohort ID to something other than the concept ID
  ncSet <- ncSet %>%
    mutate(cohortId = row_number())
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTableNames = cohortTableNames,
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "first",
      detectOnDescendants = FALSE
    )
  )

  # Get the cohort counts from the first run
  counts1 <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = ncSet
  )

  # Regenerate the negative control outcomes
  expect_output(
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTableNames = cohortTableNames,
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = ncSet,
      occurrenceType = "first",
      detectOnDescendants = FALSE
    )
  )

  counts2 <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = ncSet
  )

  expect_equal(counts1, counts2)
})

test_that("incremental mode", {
  incrementalFolder <- tempfile()
  on.exit(unlink(incrementalFolder, recursive = TRUE, force = TRUE))
  cohortTableNames <- getCohortTableNames(cohortTable = "nc_custom_cohortid")
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  ncSet <- getNegativeControlOutcomeCohortsForTest(setCohortIdToConceptId = FALSE)
  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "FINISHED")
  checkmate::expect_file_exists(file.path(incrementalFolder, "GeneratedNegativeControls.csv"))
  cohortCounts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = cohortTableNames$cohortTable
  )
  expect_equal(cohortCounts$cohortId, ncSet$cohortId)

  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = TRUE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "SKIPPED")

  # Test changing other params regenerates
  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = FALSE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "FINISHED")

  # Test changing other params regenerates
  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "first",
    detectOnDescendants = FALSE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "SKIPPED")


  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "all",
    detectOnDescendants = FALSE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "FINISHED")


  res <- generateNegativeControlOutcomeCohorts(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortTable = cohortTableNames$cohortTable,
    negativeControlOutcomeCohortSet = ncSet,
    occurrenceType = "all",
    detectOnDescendants = FALSE,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
  )

  expect_equal(res, "SKIPPED")
})
