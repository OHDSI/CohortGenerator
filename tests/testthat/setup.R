dbmsPlatforms <- c("snowflake") #c("bigquery", "oracle", "postgresql", "redshift", "spark", "sql server") # DISABLE "snowflake" tests for now
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
