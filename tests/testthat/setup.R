dbmsPlatforms <- c("redshift", "postgresql", "oracle", "sql server")
# Set the data folders before calling Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder, recursive = T, showWarnings = F)
dataFolder <- file.path(outputFolder, "eunomia_data")
dir.create(dataFolder, recursive = T, showWarnings = F)
oldEunomiaFolder <- Sys.getenv("EUNOMIA_DATA_FOLDER")
Sys.setenv("EUNOMIA_DATA_FOLDER" = dataFolder)

# Get the Eunomia connection details
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# Cleanup after testing
withr::defer(
  {
    unlink(outputFolder, recursive = TRUE, force = TRUE)
    Sys.setenv("EUNOMIA_DATA_FOLDER" = oldEunomiaFolder)
  },
  testthat::teardown_env()
)
