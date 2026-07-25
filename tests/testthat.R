library(testthat)
library(CohortGenerator)

if (identical(tolower(Sys.getenv("HADES_DATABASE_TEST", unset = "false")), "true")) {
  dbms <- getSelectedTestDbms()
  if (!nzchar(dbms)) {
    stop("HADES_DATABASE_TEST is TRUE but HADES_TEST_DBMS is not set.", call. = FALSE)
  }
  message(
    sprintf(
      "HADES_DATABASE_TEST is TRUE; running only live database tests for '%s'.",
      dbms
    )
  )
  testthat::test_file(
    system.file("testthat", "test-dbms-platforms.R", package = "CohortGenerator"),
    reporter = "summary"
  )
} else {
  test_check("CohortGenerator")
}
