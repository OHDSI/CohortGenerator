# The objective of these test is to examine SQL logic in isolation
# Testing query builders is used to test SQL taking data from manufactured target cohort tables and seeing if returned
# results of subjects are correct

test_that("Test visitOperator query", {
  tso <- createVisitSubset(name = "Test", visitConceptIds = c(1))
  qb <- VisitSubsetQb$new(tso, 1)
  checkmate::expect_r6(qb, "VisitSubsetQb")
  expect_equal(qb$getTableObjectId(), "#S_1")
  checkmate::expect_character(qb$getQuery("test"))

  memDb <- DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")
  on.exit(DatabaseConnector::disconnect(memDb))

  # neccesary tables - vist_occurrence, cohort
  cohortTable <- data.frame(
    cohortId = 1,
    subject_id = c(1,2),
    cohort_start_date = c(lubridate::date("2000-1-1"), lubridate::date("2000-1-1")),
    cohort_end_date = c(lubridate::date("2000-1-30"), lubridate::date("2000-1-30"))
  )

  DatabaseConnector::insertTable(memDb,
                                 tableName = "cohort",
                                 databaseSchema = "main",
                                 data = cohortTable)

  visitOccurrenceTable <- data.frame(
    person_id = c(1,2),
    visit_concept_id = 1,
    visit_start_date = c(lubridate::date("2000-1-1"), lubridate::date("2000-2-1")),
    #  Subject 2 has a valid visit but it isn't within the desired date range
    visit_end_date = c(lubridate::date("2000-1-1"), lubridate::date("2000-2-1"))
  )

  DatabaseConnector::insertTable(memDb,
                                 tableName = "visit_occurrence",
                                 databaseSchema = "main",
                                 data = visitOccurrenceTable)

  # Execute query
  DatabaseConnector::renderTranslateExecuteSql(memDb,
                                               qb$getQuery("cohort"),
                                               cdm_database_schema = "main")
  # Select from resulting temp table created when query step executes
  result <- DatabaseConnector::renderTranslateQuerySql(memDb,
                                                       "SELECT * FROM #S_1",
                                                       snakeCaseToCamelCase = TRUE)

  checkmate::expect_data_frame(result, nrows = 1)
  expect_equal(result$subjectId, 1)
})