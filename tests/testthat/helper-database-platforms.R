getDatabaseTestConfig <- function() {
  configFile <- system.file(
    "test-config",
    "database-platforms.yml",
    package = "CohortGenerator",
    mustWork = FALSE
  )

  if (!nzchar(configFile)) {
    configFile <- file.path(getwd(), "inst", "test-config", "database-platforms.yml")
  }

  config <- yaml::read_yaml(configFile)
  if (is.null(config$schemaVersion) || config$schemaVersion != 1) {
    stop("Unsupported database test config schema version.", call. = FALSE)
  }
  if (!identical(config$package, "CohortGenerator")) {
    stop("Database test config package must be CohortGenerator.", call. = FALSE)
  }
  config
}

getSelectedTestDbms <- function() {
  trimws(tolower(Sys.getenv("HADES_TEST_DBMS", unset = "")))
}

getDatabasePlatformConfig <- function(
    dbms = getSelectedTestDbms(),
    config = getDatabaseTestConfig()) {
  if (!nzchar(dbms)) {
    return(NULL)
  }

  matches <- vapply(
    config$platforms,
    function(platform) identical(tolower(platform$dbms), dbms),
    logical(1)
  )

  if (!any(matches)) {
    stop(
      sprintf(
        "Database platform '%s' is not declared by CohortGenerator.",
        dbms
      ),
      call. = FALSE
    )
  }

  config$platforms[[which(matches)]]
}

skipIfNoLiveDatabase <- function() {
  if (!nzchar(getSelectedTestDbms())) {
    testthat::skip("Set HADES_TEST_DBMS to run live database platform tests.")
  }
}
