dbmsPlatforms <- c("redshift", "postgresql", "oracle", "sql server")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder)
dataFolder <- file.path(outputFolder, "eunomia_data")
dir.create(dataFolder)
oldEunomiaFolder <- Sys.getenv("EUNOMIA_DATA_FOLDER")
Sys.setenv("EUNOMIA_DATA_FOLDER" = dataFolder)
withr::defer(
  {
    unlink(outputFolder)
    Sys.setenv("EUNOMIA_DATA_FOLDER" = oldEunomiaFolder)
  },
  testthat::teardown_env()
)
