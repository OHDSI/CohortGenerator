library(testthat)
library(CohortGenerator)

# Test Prep ----------------

# Helper Functions
generateSql <- function(cohortJsonFileName, generateStats = FALSE) {
  cohortExpression <- createCirceExpressionFromFile(cohortJsonFileName)
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
cohorts <- setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortName", "json", "cohortJsonFile"))
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohorts <- rbind(cohorts, data.frame(cohortId = i, 
                                       cohortName = cohortFullName, 
                                       json = cohortJson,
                                       cohortJsonFile = cohortJsonFileName,
                                       stringsAsFactors = FALSE))
}

# Exception Handling -------------
# generateCohortSet ---------
test_that("Call generateCohortSet without connection or connectionDetails", {
  expect_error(generateCohortSet(),
               message = "(connection details)")
})

test_that("Call generateCohortSet with default parameters", {
  expect_error(generateCohortSet(connectionDetails = c()),
               message = "(cohorts parameter)")
})

test_that("Call instatiateCohortSet with malformed cohortDefinitionSet parameter", {
  expect_error(generateCohortSet(connectionDetails = connectionDetails,
                                    cohortDefinitionSet = data.frame()),
               message = "(must contain the following columns)")
})

test_that("Call instatiateCohortSet with cohortDefinitionSet with extra columns", {
  cohortDefinitionSet <- createEmptyCohortDefinitionSet()
  cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(cohortId = 1,
                                           cohortName = "Test",
                                           sql = "sql",
                                           foo = "foo"))
  expect_error(generateCohortSet(connectionDetails = connectionDetails,
                                 cohortDefinitionSet = data.frame()),
               message = "(must contain the following columns)")
})

test_that("Call instatiateCohortSet with vector as cohortDefinitionSet parameter", {
  expect_error(generateCohortSet(connectionDetails = connectionDetails,
                                    cohortDefinitionSet = c()),
               message = "(data frame)")
})

test_that("Call instatiateCohortSet with incremental = TRUE and no folder specified", {
  expect_error(generateCohortSet(connectionDetails = connectionDetails,
                                    cohortDefinitionSet = createEmptyCohortDefinitionSet(),
                                    incremental = TRUE),
               message = "Must specify incrementalFolder")
})

# test_that("Ensure instatiateCohortSet is single threaded", {
#   expect_error(generateCohortSet(connectionDetails = connectionDetails,
#                                     cohortDefinitionSet = createEmptyCohortDefinitionSet(),
#                                     numThreads = 2),
#                message = "numThreads must be set to 1 for now.")
# })


# getInclusionStatistics ------
test_that("Call getInclusionStatistics without connection or connectionDetails", {
  expect_error(getInclusionStatistics(),
               message = "(connection details)")
})

# Functional Tests ----------------
test_that("Generate cohorts before creating cohort tables errors out", {
  cohortTableNames <- getCohortTableNames(cohortTable = "missing")
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  expect_error(generateCohortSet(connectionDetails = connectionDetails,
                                   cdmDatabaseSchema = "main",
                                   cohortDatabaseSchema = "main",
                                   cohortTableNames = cohortTableNames,
                                   cohortDefinitionSet = cohortsWithStats,
                                   incremental = FALSE,
                                   incrementalFolder = file.path(outputFolder, "RecordKeeping")))
})

test_that("Create cohorts with stats, Incremental = F, Gather Results", {
  outputFolder <- tempdir()
  cohortTableNames <- getCohortTableNames(cohortTable = "genStats")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTableNames = cohortTableNames,
                                        cohortDefinitionSet = cohortsWithStats,
                                        incremental = FALSE,
                                        incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  expect_equal(length(cohortsGenerated), nrow(cohortsWithStats))
  rm(cohortsWithStats)
  unlink(outputFolder)
})

test_that("Create cohorts with stats, Incremental = T", {
  outputFolder <- tempdir()
  cohortTableNames <- getCohortTableNames(cohortTable = "genStatsInc")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  # 1st run first to ensure that all cohorts are generated
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTableNames = cohortTableNames,
                                           cohortDefinitionSet = cohortsWithStats,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  # 2nd run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTableNames = cohortTableNames,
                                        cohortDefinitionSet = cohortsWithStats,
                                        incremental = TRUE,
                                        incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  expect_equal(length(cohortsGenerated), nrow(cohortsWithStats))
  rm(cohortsWithStats)
  unlink(outputFolder)
})

test_that("Create cohorts without stats, Incremental = F", {
  outputFolder <- tempdir()
  cohortTableNames <- getCohortTableNames(cohortTable = "noStats")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = "main",
                                        cohortDatabaseSchema = "main",
                                        cohortTableNames = cohortTableNames,
                                        cohortDefinitionSet = cohortsWithoutStats,
                                        incremental = FALSE,
                                        incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  expect_equal(length(cohortsGenerated), nrow(cohortsWithoutStats))
  rm(cohortsWithoutStats)
  unlink(outputFolder)
})

test_that("Create cohorts without stats, Incremental = T", {
  outputFolder <- tempdir()
  cohortTableNames <- getCohortTableNames(cohortTable = "noStatsInc")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  # Run first to ensure that all cohorts are generated
  cohortsWithoutStats <- getCohortsForTest(cohorts, generateStats = FALSE)
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTableNames = cohortTableNames,
                                           cohortDefinitionSet = cohortsWithoutStats,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  # Next run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- generateCohortSet(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = "main",
                                           cohortDatabaseSchema = "main",
                                           cohortTableNames = cohortTableNames,
                                           cohortDefinitionSet = cohortsWithoutStats,
                                           incremental = TRUE,
                                           incrementalFolder = file.path(outputFolder, "RecordKeeping"))
  expect_equal(length(cohortsGenerated), nrow(cohortsWithoutStats))
  unlink(outputFolder)
})

# Test Cohort Stats ----------------
test_that("Insert cohort stats expected use-case", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_insert")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)

  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)

  # Insert the inclusion rule names
  cohortInclusionRules <- insertInclusionRuleNames(connectionDetails = connectionDetails,
                                                   cohortDefinitionSet = cohortsWithStats,
                                                   cohortDatabaseSchema = "main",
                                                   cohortInclusionTable = cohortTableNames$cohortInclusionTable)

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                        sql = "SELECT * FROM @cohort_database_schema.@table",
                                                        cohort_database_schema = "main",
                                                        table = cohortTableNames$cohortInclusionTable,
                                                        snakeCaseToCamelCase = TRUE)
  expect_equal(results, cohortInclusionRules)
  DatabaseConnector::disconnect(conn)
})

test_that("Insert cohort stats missing connection info", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_missing_conn")
  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  
  # Expect an error
  expect_error(insertInclusionRuleNames(cohortDefinitionSet = cohortsWithStats,
                                        cohortDatabaseSchema = "main",
                                        cohortInclusionTable = cohortTableNames$cohortInclusionTable))
})

test_that("Insert cohort stats before creating cohort tables", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_tables_missing")
  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  
  # Expect an error
  expect_error(insertInclusionRuleNames(connectionDetails = connectionDetails,
                                        cohortDefinitionSet = cohortsWithStats,
                                        cohortDatabaseSchema = "main",
                                        cohortInclusionTable = cohortTableNames$cohortInclusionTable))
})

test_that("Insert cohort stats with inclusion rule name that is empty", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_name_empty")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)

  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  # Change the cohort definition so the inclusion rule is empty
  cohortDefinition <- RJSONIO::fromJSON(content = cohortsWithStats$json[2], digits = 23)
  cohortDefinition$InclusionRules[[1]]$name <- ""
  cohortsWithStats$json[2] <- RJSONIO::toJSON(cohortDefinition)

  # Insert the inclusion rule names
  cohortInclusionRules <- insertInclusionRuleNames(connectionDetails = connectionDetails,
                                                   cohortDefinitionSet = cohortsWithStats,
                                                   cohortDatabaseSchema = "main",
                                                   cohortInclusionTable = cohortTableNames$cohortInclusionTable)

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  results <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                        sql = "SELECT * FROM @cohort_database_schema.@table",
                                                        cohort_database_schema = "main",
                                                        table = cohortTableNames$cohortInclusionTable,
                                                        snakeCaseToCamelCase = TRUE)
  expect_equal(results, cohortInclusionRules)
  DatabaseConnector::disconnect(conn)
})

test_that("Insert cohort stats with no inclusion rules generates warning", {
  # Create the cohort tables
  cohortTableNames <- getCohortTableNames(cohortTable = "stats_missing")
  createCohortTables(connectionDetails = connectionDetails,
                     cohortDatabaseSchema = "main",
                     cohortTableNames = cohortTableNames)
  
  # Obtain a list of cohorts with inclusion rule stats
  cohortsWithStats <- getCohortsForTest(cohorts, generateStats = TRUE)
  # The 1st cohort definition lacks inclusion rules
  cohortsWithStats <- cohortsWithStats[1,]

  # Insert the inclusion rule names
  expect_warning(insertInclusionRuleNames(connectionDetails = connectionDetails,
                                          cohortDefinitionSet = cohortsWithStats,
                                          cohortDatabaseSchema = "main",
                                          cohortInclusionTable = cohortTableNames$cohortInclusionTable))
})

# Cleanup ------
rm(generateSql)
rm(getCohortsForTest)
rm(cohortJsonFiles)
rm(cohorts)
