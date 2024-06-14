dbmsPlatforms <- c() # c("bigquery", "oracle", "postgresql", "redshift", "snowflake", "spark", "sql server")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
