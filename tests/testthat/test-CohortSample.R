
test_that("sampleCohortDefinitionSet", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  cohortTableNames <- getCohortTableNames(cohortTable = "cohort",
                                          cohortSampleTable = "cohort_sample")
  recordKeepingFolder <- file.path(outputFolder, "RecordKeepingSamples")

  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  cds <- getCohortsForTest(cohorts = cohorts)
  generateCohortSet(
    cohortDefinitionSet = cds,
    connection = conn,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )

  sampledCohorts <- sampleCohortDefinitionSet(
    cohortDefinitionSet = cds,
    connection = conn,
    n = 10,
    seed = 64374,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  checkmate::expect_data_frame(sampledCohorts, nrow = nrow(cds))
  expect_true(all(grepl("[SAMPLE seed=64374 n=10]", sampledCohorts$cohortName)))
  expect_true(all(cds$cohortId * 1000 + 64374 == sampledCohorts$cohortId))

  # Sample table pouplated
  res <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                    "SELECT cohort_definition_id, count(*) as ct FROM cohort_sample
                                                    GROUP BY cohort_definition_id
                                                    ")
  expect_true(all(res$ct == 10))
})

# Testing the creation of randomly sampled without replacement row ids
test_that(".getSampleSet", {
  connection <- DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")
  on.exit(DatabaseConnector::disconnect(connection))

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "cohort",
                                 camelCaseToSnakeCase = TRUE,
                                 data = data.frame(cohortDefinitionId = 1,
                                                   subjectId = 1:1e5))

  cohortDatabaseSchema <- "main"
  targetTable <- "cohort"
  targetCohortId <- 1
  n <- 100
  seed <- 1
  seedArgs <- NULL

  res <- .getSampleSet(connection,
                       n,
                       seed,
                       seedArgs,
                       cohortDatabaseSchema,
                       targetCohortId,
                       targetTable)

  checkmate::expect_data_frame(res, types = "integer", nrows = n)

  res2 <- .getSampleSet(connection,
                        n,
                        seed,
                        seedArgs,
                        cohortDatabaseSchema,
                        targetCohortId,
                        targetTable)
  # use of the same seed should produce the same result
  expect_true(all(res$rand_id == res2$rand_id))

  res2 <- .getSampleSet(connection,
                        n,
                        seed + 1,
                        seedArgs,
                        cohortDatabaseSchema,
                        targetCohortId,
                        targetTable)

  expect_false(all(res$rand_id == res2$rand_id))
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "cohort",
                                 camelCaseToSnakeCase = TRUE,
                                 data = data.frame(cohortDefinitionId = 2,
                                                   subjectId = 1:25))
  # Where n > count should return count rows
  res3 <- .getSampleSet(connection,
                        n,
                        seed,
                        seedArgs,
                        cohortDatabaseSchema,
                        targetCohortId = 2,
                        targetTable)
  checkmate::expect_data_frame(res3, types = "integer", nrows = 25)
})

# Testing that selection of rows is determined only by the inserted random data.frame
test_that(".sampleCohort", {
  connection <- DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")
  on.exit(DatabaseConnector::disconnect(connection))

  cohortCount <- 1000
  startDates <- sample(seq(as.Date('2001/01/01'), as.Date('2023/01/01'), by = "day"), cohortCount)
  endDates <- startDates + sample(1:800, cohortCount, replace = TRUE)

  tData <- data.frame(cohortDefinitionId = 1,
                      subjectId = 1:cohortCount,
                      cohortStartDate = startDates,
                      cohortEndDate = endDates)
  # dupes ensures that dense_rank allows selection of multiple cohort entries for the same subject
  tData <- rbind(tData, tData)

  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "cohort",
                                 camelCaseToSnakeCase = TRUE,
                                 data = tData)
  sampleTable <- data.frame(rand_id = c(7, 8, 9, 10, 33, 198))
  .sampleCohort(connection,
                targetCohortId = 1,
                targetTable = "cohort",
                outputCohortId = 999,
                outputTable = "cohort",
                cohortDatabaseSchema = "main",
                outputDatabaseSchema = "main",
                sampleTable = sampleTable,
                seed = 1,
                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"))

  resCohort <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                          "SELECT * FROM main.cohort WHERE cohort_definition_id = 999",
                                                          snakeCaseToCamelCase = TRUE)

  checkmate::expect_data_frame(resCohort, nrows = nrow(sampleTable) * 2)
  expect_true(all(resCohort$subjectId %in% sampleTable$rand_id))
})

test_that("Eval expression is safe", {
  expect_true(.computeIdentifierExpression("cohortId + seed", 1, 5) == 6)
  expect_true(.computeIdentifierExpression("seed", 1, 55) == 55)
  expect_true(.computeIdentifierExpression("cohortId", 1, 55) == 1)
  foo <- 5
  expect_error(.computeIdentifierExpression("foo"), "Invalid variable in expression")
})