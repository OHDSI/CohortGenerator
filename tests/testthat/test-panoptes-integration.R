Panoptes::skipIfLiveDatabaseTest()

test_that("Panoptes finds the installed CohortGenerator declaration", {
  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")

  expect_equal(config$package, "CohortGenerator")
  expect_equal(config$databaseConnection, "all")
  expect_equal(
    vapply(config$platforms, `[[`, character(1), "dbms"),
    c(
      "sqlite", "duckdb", "postgresql", "sql server", "oracle",
      "redshift", "spark", "bigquery", "snowflake", "iris"
    )
  )
})

test_that("CohortGenerator declares all external database platforms", {
  config <- Panoptes::readDatabaseTestConfig(package = "CohortGenerator")
  platforms <- Panoptes::getConfiguredDatabasePlatforms(config)
  externalPlatforms <- Filter(
    function(platform) !identical(platform$connectionMode, "local"),
    platforms
  )

  expect_equal(
    vapply(externalPlatforms, `[[`, character(1), "dbms"),
    c(
      "postgresql", "sql server", "oracle", "redshift", "spark",
      "bigquery", "snowflake", "iris"
    )
  )
})
