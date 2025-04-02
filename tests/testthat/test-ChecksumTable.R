test_that("getLastGeneratedCohortChecksums throws error if no connection or connectionDetails provided", {
  expect_error(
    getLastGeneratedCohortChecksums(
      connectionDetails = NULL,
      connection = NULL,
      cohortDatabaseSchema = "foo"
    ),
    "You must provide either a database connection or the connection details."
  )
})

test_that("getLastGeneratedCohortChecksums tables not created", {
  expect_error(
    res <- getLastGeneratedCohortChecksums(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = "main"
    )
  )
})

test_that("getLastGeneratedCohortChecksums returns correct data frame", {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  cts <- getCohortTableNames("checksum_test")
  createCohortTables(connection = connection, cohortTableNames = cts, cohortDatabaseSchema = "main")

  sql <- "SELECT NULL;"
  startTime <- lubridate::now()
  # Generate the same cohort id twice, expect only one row to be returned
  .runCohortSql(connection, sql, startTime, "main", cts$cohortChecksumTable, FALSE, 1, "ABC", "file.out")

  startTime <- lubridate::now()
  .runCohortSql(connection, sql, startTime, "main", cts$cohortChecksumTable, FALSE, 1, "ABCDEFG", "file.out")

  result <- getLastGeneratedCohortChecksums(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cts
  )
  expect_s3_class(result, "data.frame")
  # expect 2 different entries in checksum log
  expect_equal(nrow(result), 1)
  expect_equal(result$cohortDefinitionId, 1)
  expect_equal(result$checksum, "ABCDEFG")

})