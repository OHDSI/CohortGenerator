test_that("combination cohort generation", {
  
  recordKeepingFolder = file.path(outputFolder, "RecordKeeping")
  
  createCohortSql <- function (subjectId, cohortStartDate, cohortEndDate) {
    templateSql = 
"insert into @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
  values (@target_cohort_id, @subject_id, CAST('@cohort_start_date' AS DATE), CAST('@cohort_end_date' AS DATE));"
    return (SqlRender::render(templateSql, 
                              subject_id = subjectId, 
                              cohort_start_date = cohortStartDate, 
                              cohort_end_date = cohortEndDate))
  }
  
  testCohortDefinitionSet <- data.frame(cohortId = c(50:52),
                                        cohortName = paste0('Cohort ',c(50:52)),
                                        sql=c(
                                          createCohortSql(1, "20160101", "20160301"),
                                          createCohortSql(1, "20160201", "20160401"),
                                          createCohortSql(1, "20160301", "20160501")
                                        ),
                                        json=c("{}","{}","{}"))
  
  cohortDefinitionSet <- rbind(CohortGenerator::createEmptyCohortDefinitionSet(), testCohortDefinitionSet)

  #build cohort and combined cohort defs
  
  combinedCohortOp <- CohortGenerator::createCombinedCohortOp(targetCohortIds = c(50,51,52), opType = "union")
  combinedCohortDef <- CohortGenerator::createCombinedCohortDef(cohortId = 53, cohortName="Cohort 50,51,52 combo", expression = combinedCohortOp)

  cohortDefinitionSet <- cohortDefinitionSet %>%
    CohortGenerator::addCombinedCohort(combinedCohortDef)
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "combinedCohorts_cohort")
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  CohortGenerator::createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  cohortsGenerated <- CohortGenerator::generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE,
    incrementalFolder = recordKeepingFolder
  )

  # how to test
  resultSql <- "select cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM main.@cohort_table where cohort_definition_id = 53"
  resultSql <- SqlRender::render(resultSql, cohort_table = cohortTableNames$cohortTable) 
  resultsDF <- DatabaseConnector::querySql(connection = connection, SqlRender::translate(resultSql, targetDialect = "sqlite"))
  
  expect_true(nrow(resultsDF) == 1)
  expect_true(resultsDF %>% dplyr::select("COHORT_START_DATE") %>% dplyr::pull() == "2016-01-01")
  expect_true(resultsDF %>% dplyr::select("COHORT_END_DATE") %>% dplyr::pull() == "2016-05-01")
  
  unlink(recordKeepingFolder, recursive = TRUE)
  
})