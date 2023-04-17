dbmsPlatforms <- c("redshift", "postgresql", "oracle", "sql server")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
