dbmsPlatforms <- c("redshift", "postgresql", "oracle", "sql server")
# Set the data folders before calling Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempfile()
dir.create(outputFolder, recursive = T, showWarnings = F)
dataFolder <- file.path(outputFolder, "eunomia_data")
dir.create(dataFolder, recursive = T, showWarnings = F)
oldEunomiaFolder <- Sys.getenv("EUNOMIA_DATA_FOLDER")
Sys.setenv("EUNOMIA_DATA_FOLDER" = dataFolder)
eunomiaDbFile <- file.path(outputFolder, "data", "testEunomia.sqlite")
if (!dir.exists(file.path(outputFolder, "data"))) {
  dir.create(file.path(outputFolder, "data"), recursive = T, showWarnings = F)
}
connectionDetails <- Eunomia::getEunomiaConnectionDetails(
  databaseFile = eunomiaDbFile
)

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
withr::defer(
  {
    unlink(outputFolder, recursive = TRUE, force = TRUE)
    unlink(eunomiaDbFile, recursive = TRUE, force = TRUE)
    Sys.setenv("EUNOMIA_DATA_FOLDER" = oldEunomiaFolder)
  },
  testthat::teardown_env()
)
