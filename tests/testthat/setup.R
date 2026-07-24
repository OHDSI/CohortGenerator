# The live database workflow sources this file too, but that workflow only runs
# the DBMS-specific test file and does not need the Eunomia fixture.
if (!nzchar(Sys.getenv("HADES_TEST_DBMS", unset = ""))) {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
} else {
  connectionDetails <- NULL
}
outputFolder <- tempfile()
dir.create(outputFolder)
withr::defer(
  {
    unlink(outputFolder)
  },
  testthat::teardown_env()
)
