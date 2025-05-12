connection <- DatabaseConnector::connect(connectionDetails)

withr::defer({
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())

test_that("Test Basic Template", {
  junkSql <- "
  {DEFAULT @cdm_database_schema = cdm}
  {DEFAULT @vocabulary_database_schema = @cdm_database_schema}

  INSERT INTO @cohort_database_schema.@cohort_table
  (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
  SELECT 1 as cohort_definition_id, 1 as subject_id, '10/10/2020'as cohort_start_date, '@end_date' as cohort_end_date
  UNION
  SELECT 1 as cohort_definition_id, 2 as subject_id, '10/10/2014' as cohort_start_date, '@end_date2' as cohort_end_date;
  "
  tplDef <- createCohortTemplateDefintion(name = "test template",
                                          templateSql = junkSql,
                                          references = data.frame(cohortId = 1, cohortName = "one"),
                                          sqlArgs = list(end_date = '01/01/2021', end_date2 = '01/01/2023'),
                                          translateSql = TRUE)
  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")


  cohortDefinitionSet <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef)
  testOutputFolder <- file.path(outputFolder, "tpl_tests")
  cohortTableNames <- CohortGenerator::getCohortTableNames("cohort_tpl")

  createCohortTables(connection = connection,
                     cohortTableNames = cohortTableNames,
                     cohortDatabaseSchema = "main")

  generateCohortSet(connection = connection,
                    cdmDatabaseSchema = "main",
                    cohortDatabaseSchema = "main",
                    cohortTableNames = cohortTableNames,
                    cohortDefinitionSet = cohortDefinitionSet,
                    stopOnError = TRUE,
                    incremental = TRUE,
                    incrementalFolder = testOutputFolder)

  # check the count is 2
  count <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = "cohort_tpl",
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = "Eunomia"
  )

  expect_equal(count$cohortSubjects, 2)
  expect_equal(count$cohortEntries, 2)

  status <- generateCohortSet(connection = connection,
                              cdmDatabaseSchema = "main",
                              cohortDatabaseSchema = "main",
                              cohortTableNames = cohortTableNames,
                              cohortDefinitionSet = cohortDefinitionSet,
                              stopOnError = TRUE,
                              incremental = TRUE,
                              incrementalFolder = testOutputFolder)
  expect_equal(status$generationStatus, "SKIPPED")
})


test_that("Stop with a bad cohort", {

  sql <- "SOME BAD SQL"

  cohortDefinitionSet <- createEmptyCohortDefinitionSet() |>
    addSqlCohortDefinition(sql = sql,
                           cohortId = 1,
                           cohortName = "my BAD cohort",
                           warnOnMissingParameters = FALSE)

  checkmate::expect_data_frame(cohortDefinitionSet)
  cohortTableNames <- CohortGenerator::getCohortTableNames("cohort_tpl2")
  createCohortTables(connection = connection,
                     cohortTableNames = cohortTableNames,
                     cohortDatabaseSchema = "main")
  expect_error({
    generateCohortSet(connection = connection,
                      cdmDatabaseSchema = "main",
                      cohortDatabaseSchema = "main",
                      cohortTableNames = cohortTableNames,
                      cohortDefinitionSet = cohortDefinitionSet,
                      stopOnError = TRUE,
                      incremental = FALSE,
                      incrementalFolder = NULL)
  })

  # No Error
  generateCohortSet(connection = connection,
                      cdmDatabaseSchema = "main",
                      cohortDatabaseSchema = "main",
                      cohortTableNames = cohortTableNames,
                      cohortDefinitionSet = cohortDefinitionSet,
                      stopOnError = FALSE,
                      incremental = FALSE,
                      incrementalFolder = NULL)
})