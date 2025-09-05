dbmsPlatforms <- c("bigquery", "oracle", "postgresql", "redshift", "spark", "sql server") # DISABLE "snowflake" test for now
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
