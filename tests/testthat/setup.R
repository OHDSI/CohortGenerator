dbmsPlatforms <- c("oracle", "postgresql", "redshift", "spark", "sql server") # DISABLE "bigquery", "snowflake" tests for now
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
