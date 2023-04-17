library(testthat)
library(CohortGenerator)

# Exception Handling -------------
# generateCohortSet ---------
test_that("Call generateCohortSet without connection or connectionDetails", {
  expect_error(generateCohortSet(cohortDefinitionSet = getCohortsForTest(cohorts)),
    message = "(connection details)"
  )
})

test_that("Call generateCohortSet with default parameters", {
  expect_error(
    generateCohortSet(
      cohortDefinitionSet = getCohortsForTest(cohorts),
      connectionDetails = c()
    ),
    message = "(cohorts parameter)"
  )
})

test_that("Call instatiateCohortSet with malformed cohortDefinitionSet parameter", {
  expect_error(
    generateCohortSet(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = data.frame()
    ),
    message = "(must contain the following columns)"
  )
})

test_that("Call instatiateCohortSet with cohortDefinitionSet with extra columns", {
  cohortDefinitionSet <- createEmptyCohortDefinitionSet()
  cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(
    cohortId = 1,
    cohortName = "Test",
    sql = "sql",
    foo = "foo"
  ))
  expect_error(
    generateCohortSet(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = data.frame()
    ),
    message = "(must contain the following columns)"
  )
})

test_that("Call instatiateCohortSet with vector as cohortDefinitionSet parameter", {
  expect_error(
    generateCohortSet(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = c()
    ),
    message = "(data frame)"
  )
})

test_that("Call instatiateCohortSet with incremental = TRUE and no folder specified", {
  expect_error(
    generateCohortSet(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = getCohortsForTest(cohorts),
      incremental = TRUE
    ),
    message = "Must specify incrementalFolder"
  )
})

# getInclusionStatistics ------
test_that("Call getInclusionStatistics without connection or connectionDetails", {
  expect_error(getInclusionStatistics(),
    message = "(connection details)"
  )
})

# Functional Tests ----------------
test_that("Generate cohorts before creating cohort tables errors out", {
  cohortTableNames <- getCohortTableNames(cohortTable = "missing")
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  expect_error(generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats,
    incremental = FALSE,
    incrementalFolder = file.path(outputFolder, "RecordKeeping")
  ))
})

test_that("Create cohorts with stats, Incremental = F, Gather Results", {
  cohortTableNames <- getCohortTableNames(cohortTable = "genStats")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats,
    incremental = FALSE,
    incrementalFolder = file.path(outputFolder, "RecordKeeping")
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithStats))
  rm(cohortsWithStats)
})

test_that("Create cohorts with stats, Incremental = T", {
  recordKeepingFolder <- file.path(outputFolder, "RecordKeeping")
  cohortTableNames <- getCohortTableNames(cohortTable = "genStatsInc")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  # 1st run first to ensure that all cohorts are generated
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  # 2nd run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithStats,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithStats))
  expect_true(all(cohortsGenerated$generationStatus == "SKIPPED"))
  rm(cohortsWithStats)
  unlink(recordKeepingFolder, recursive = TRUE)
})

test_that("Create cohorts without stats, Incremental = F", {
  cohortTableNames <- getCohortTableNames(cohortTable = "noStats")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    incremental = FALSE,
    incrementalFolder = file.path(outputFolder, "RecordKeeping")
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithoutStats))
  rm(cohortsWithoutStats)
})

test_that("Create cohorts without stats, Incremental = T", {
  recordKeepingFolder <- file.path(outputFolder, "RecordKeeping")
  cohortTableNames <- getCohortTableNames(cohortTable = "noStatsInc")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  # Next run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithoutStats))
  unlink(recordKeepingFolder, recursive = TRUE)
})

test_that("Create cohorts with stopOnError = TRUE", {
  cohortTableNames <- getCohortTableNames(cohortTable = "stop_error_t")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  # Add a new cohort that will automatically fail
  cohortsWithoutStats <- rbind(cohortsWithoutStats, data.frame(
    atlasId = 999,
    cohortId = 999,
    cohortName = "Fail Cohort",
    json = "",
    cohortJsonFile = "",
    sql = "SELECT * FROM @cdm_database_schema.non_existant_table"
  ))
  expect_error(generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    stopOnError = TRUE
  ))
  if (file.exists("errorReportSql.txt")) {
    unlink("errorReportSql.txt")
  }
})

test_that("Create cohorts with stopOnError = FALSE", {
  print("Create cohorts with stopOnError = FALSE")
  cohortTableNames <- getCohortTableNames(cohortTable = "stop_error_f")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  # Add a new cohort that will automatically fail
  cohortsWithoutStats <- rbind(data.frame(
    atlasId = 999,
    cohortId = 999,
    cohortName = "Fail Cohort",
    json = "",
    cohortJsonFile = "",
    sql = "SELECT * FROM @cdm_database_schema.non_existant_table"
  ), cohortsWithoutStats)
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    stopOnError = FALSE
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithoutStats))
  expect_equal(nrow(cohortsGenerated[cohortsGenerated$generationStatus == "FAILED", ]), 1)
  if (file.exists("errorReportSql.txt")) {
    unlink("errorReportSql.txt")
  }
})

test_that("Create cohorts with stopOnError = FALSE and incremental = TRUE", {
  recordKeepingFolder <- file.path(outputFolder, "RecordKeeping")
  cohortTableNames <- getCohortTableNames(cohortTable = "stop_error_f_inc_t")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  # Add a new cohort that will automatically fail
  cohortsWithoutStats <- rbind(data.frame(
    atlasId = 999,
    cohortId = 999,
    cohortName = "Fail Cohort",
    json = "",
    cohortJsonFile = "",
    sql = "SELECT * FROM @cdm_database_schema.non_existant_table"
  ), cohortsWithoutStats)
  # Generate the cohorts expecting that 1 will fail and 3 will succeed
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    stopOnError = FALSE,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithoutStats))
  expect_equal(nrow(cohortsGenerated[cohortsGenerated$generationStatus == "FAILED", ]), 1)
  expect_equal(nrow(cohortsGenerated[cohortsGenerated$generationStatus == "COMPLETE", ]), 3)

  # Now update the cohort that was failing to use a SQL statement that will work
  sqlThatWillWork <- "
    INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    SELECT @target_cohort_id, person_id, observation_period_start_date, observation_period_end_date
    FROM @cdm_database_schema.observation_period
    ;"
  cohortsWithoutStats[cohortsWithoutStats$cohortId == 999, ]$sql <- sqlThatWillWork
  # Generate the cohorts expecting that 1 will succeed and 3 will be skipped
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortsWithoutStats,
    stopOnError = FALSE,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortsWithoutStats))
  expect_equal(nrow(cohortsGenerated[cohortsGenerated$generationStatus == "COMPLETE", ]), 1)
  expect_equal(nrow(cohortsGenerated[cohortsGenerated$generationStatus == "SKIPPED", ]), 3)
  unlink(recordKeepingFolder, recursive = TRUE)
  if (file.exists("errorReportSql.txt")) {
    unlink("errorReportSql.txt")
  }
})


# Test Cohort Stats ----------------
test_that("Insert cohort stats expected use-case", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_insert")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

  # Insert the inclusion rule names
  cohortInclusionRules <- insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  )

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = conn,
    sql = "SELECT * FROM @cohort_database_schema.@table",
    cohort_database_schema = "main",
    table = cohortTableNames$cohortInclusionTable,
    snakeCaseToCamelCase = TRUE
  )
  # HACK: SqlLite does not support bigint so convert the results returned
  results$cohortDefinitionId <- bit64::as.integer64(results$cohortDefinitionId)
  expect_equal(results, cohortInclusionRules)
  DatabaseConnector::disconnect(conn)
})

test_that("Insert cohort stats missing connection info", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_missing_conn")
  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

  # Expect an error
  expect_error(insertInclusionRuleNames(
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  ))
})

test_that("Insert cohort stats before creating cohort tables", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_tables_missing")
  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

  # Expect an error
  expect_error(insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  ))
})

test_that("Insert cohort stats with inclusion rule name that is empty", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_name_empty")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  # Change the cohort definition so the inclusion rule is empty
  cohortDefinition <- RJSONIO::fromJSON(content = cohortsWithStats$json[2], digits = 23)
  cohortDefinition$InclusionRules[[1]]$name <- ""
  cohortsWithStats$json[2] <- RJSONIO::toJSON(cohortDefinition)

  # Insert the inclusion rule names
  cohortInclusionRules <- insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  )

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = conn,
    sql = "SELECT * FROM @cohort_database_schema.@table",
    cohort_database_schema = "main",
    table = cohortTableNames$cohortInclusionTable,
    snakeCaseToCamelCase = TRUE
  )
  # HACK: SqlLite does not support bigint so convert the results returned
  results$cohortDefinitionId <- bit64::as.integer64(results$cohortDefinitionId)
  expect_equal(results, cohortInclusionRules)
  DatabaseConnector::disconnect(conn)
})

test_that("Insert cohort stats with no inclusion rules generates warning", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_missing")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  # The 1st cohort definition lacks inclusion rules
  cohortsWithStats <- cohortsWithStats[1, ]

  # Insert the inclusion rule names
  expect_warning(insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  ))
})

test_that("Insert cohort stats with INT64 for cohort_definition_id", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_bigint")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  # Obtain a list of cohorts to test
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

  # Hack the cohortDefinitionId to force to 64 bit integer
  cohortsWithStats$cohortId <- bit64::as.integer64(cohortsWithStats$cohortId)
  cohortsWithStats$cohortId <- cohortsWithStats$cohortId + .Machine$integer.max

  # Insert the inclusion rule names
  cohortInclusionRules <- insertInclusionRuleNames(
    connectionDetails = connectionDetails,
    cohortDefinitionSet = cohortsWithStats,
    cohortDatabaseSchema = "main",
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  )

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = conn,
    sql = "SELECT * FROM @cohort_database_schema.@table",
    cohort_database_schema = "main",
    table = cohortTableNames$cohortInclusionTable,
    snakeCaseToCamelCase = TRUE
  )
  # HACK: SqlLite does not support bigint so convert the results returned
  results$cohortDefinitionId <- bit64::as.integer64(results$cohortDefinitionId)
  expect_equal(results, cohortInclusionRules)
  DatabaseConnector::disconnect(conn)
})
