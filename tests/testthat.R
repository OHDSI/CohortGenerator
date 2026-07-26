library(testthat)
library(CohortGenerator)

if (identical(tolower(Sys.getenv("HADES_DATABASE_TEST", unset = "false")), "true")) {
  dbms <- trimws(Sys.getenv("HADES_TEST_DBMS", unset = ""))
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
    file.path("tests", "testthat", "test-dbms-platforms.R"),
    reporter = "summary"
  )
} else {
  test_check("CohortGenerator")
}
