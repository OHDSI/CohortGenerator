getDatabaseTestConfig <- function() {
  configFile <- system.file(
    "test-config",
    "database-platforms.yml",
    package = "CohortGenerator",
    mustWork = FALSE
  )

  if (!nzchar(configFile) || !file.exists(configFile)) {
    configFile <- file.path(getwd(), "inst", "test-config", "database-platforms.yml")
  }

  config <- yaml::read_yaml(configFile)
  if (is.null(config$schemaVersion) || config$schemaVersion != 1) {
    stop("Unsupported database test config schema version.", call. = FALSE)
  }
  if (!identical(config$package, "CohortGenerator")) {
    stop("Database test config package must be CohortGenerator.", call. = FALSE)
  }
  if (is.null(config$databaseConnection) || !config$databaseConnection %in% c("none", "subset", "all")) {
    stop(
      "Database test config databaseConnection must be one of none, subset, or all.",
      call. = FALSE
    )
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

getJdbcDriverFolder <- function() {
  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- file.path(path.expand("~"), ".jdbcDrivers")
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
    jdbcDriverFolder
  }
}

getPostgresqlDatabaseEnvironmentVariables <- function() {
  c(
    "CDM5_POSTGRESQL_USER",
    "CDM5_POSTGRESQL_PASSWORD",
    "CDM5_POSTGRESQL_SERVER",
    "CDM5_POSTGRESQL_CDM_SCHEMA",
    "CDM5_POSTGRESQL_OHDSI_SCHEMA"
  )
}

getOracleDatabaseEnvironmentVariables <- function() {
  c(
    "CDM5_ORACLE_USER",
    "CDM5_ORACLE_PASSWORD",
    "CDM5_ORACLE_SERVER",
    "CDM5_ORACLE_CDM_SCHEMA",
    "CDM5_ORACLE_OHDSI_SCHEMA"
  )
}

getRedshiftDatabaseEnvironmentVariables <- function() {
  c(
    "CDM5_REDSHIFT_USER",
    "CDM5_REDSHIFT_PASSWORD",
    "CDM5_REDSHIFT_SERVER",
    "CDM5_REDSHIFT_CDM_SCHEMA",
    "CDM5_REDSHIFT_OHDSI_SCHEMA"
  )
}

getSparkDatabaseEnvironmentVariables <- function() {
  c(
    "CDM5_SPARK_USER",
    "CDM5_SPARK_PASSWORD",
    "CDM5_SPARK_CONNECTION_STRING",
    "CDM5_SPARK_CDM_SCHEMA",
    "CDM5_SPARK_OHDSI_SCHEMA"
  )
}

getSqlServerDatabaseEnvironmentVariables <- function() {
  c(
    "CDM5_SQL_SERVER_USER",
    "CDM5_SQL_SERVER_PASSWORD",
    "CDM5_SQL_SERVER_SERVER",
    "CDM5_SQL_SERVER_CDM_SCHEMA",
    "CDM5_SQL_SERVER_OHDSI_SCHEMA"
  )
}

getBigQueryDatabaseEnvironmentVariables <- function() {
  c(
    "CDM_BIG_QUERY_KEY_FILE",
    "CDM_BIG_QUERY_CONNECTION_STRING",
    "CDM_BIG_QUERY_CDM_SCHEMA",
    "CDM_BIG_QUERY_OHDSI_SCHEMA"
  )
}

getSnowflakeDatabaseEnvironmentVariables <- function() {
  c(
    "CDM_SNOWFLAKE_USER",
    "CDM_SNOWFLAKE_PASSWORD",
    "CDM_SNOWFLAKE_CONNECTION_STRING",
    "CDM_SNOWFLAKE_CDM53_SCHEMA",
    "CDM_SNOWFLAKE_OHDSI_SCHEMA"
  )
}

getPostgresqlConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder
  )
}

getOracleConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "oracle",
    user = Sys.getenv("CDM5_ORACLE_USER"),
    password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
    server = Sys.getenv("CDM5_ORACLE_SERVER"),
    pathToDriver = jdbcDriverFolder
  )
}

getRedshiftConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "redshift",
    user = Sys.getenv("CDM5_REDSHIFT_USER"),
    password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
    server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
    pathToDriver = jdbcDriverFolder
  )
}

getSparkConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    user = Sys.getenv("CDM5_SPARK_USER"),
    password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD")),
    connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
    pathToDriver = jdbcDriverFolder
  )
}

getSqlServerConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "sql server",
    user = Sys.getenv("CDM5_SQL_SERVER_USER"),
    password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
    server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
    pathToDriver = jdbcDriverFolder
  )
}

getBigQueryConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  bqKeyFile <- tempfile(fileext = ".json")
  writeLines(Sys.getenv("CDM_BIG_QUERY_KEY_FILE"), bqKeyFile)
  if (testthat::is_testing()) {
    withr::defer(unlink(bqKeyFile, force = TRUE), testthat::teardown_env())
  }
  bqConnectionString <- gsub(
    "<keyfile path>",
    normalizePath(bqKeyFile, winslash = "/"),
    Sys.getenv("CDM_BIG_QUERY_CONNECTION_STRING")
  )
  DatabaseConnector::createConnectionDetails(
    dbms = "bigquery",
    user = "",
    password = "",
    connectionString = !!bqConnectionString,
    pathToDriver = jdbcDriverFolder
  )
}

getRequiredDatabaseEnvironmentVariables <- function(dbmsPlatform) {
  switch(
    dbmsPlatform,
    postgresql = getPostgresqlDatabaseEnvironmentVariables(),
    oracle = getOracleDatabaseEnvironmentVariables(),
    redshift = getRedshiftDatabaseEnvironmentVariables(),
    spark = getSparkDatabaseEnvironmentVariables(),
    "sql server" = getSqlServerDatabaseEnvironmentVariables(),
    bigquery = getBigQueryDatabaseEnvironmentVariables(),
    snowflake = getSnowflakeDatabaseEnvironmentVariables(),
    stop(sprintf("Unsupported DBMS '%s'.", dbmsPlatform), call. = FALSE)
  )
}

validateDatabaseTestEnvironment <- function(dbmsPlatform, requiredVariables = getRequiredDatabaseEnvironmentVariables(dbmsPlatform)) {
  missingVariables <- requiredVariables[!nzchar(Sys.getenv(requiredVariables, unset = ""))]

  if (length(missingVariables) > 0) {
    stop(
      paste0(
        "Missing environment variables for ",
        dbmsPlatform,
        ": ",
        paste(missingVariables, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

resolveDatabasePlatformSettings <- function(dbmsPlatform, jdbcDriverFolder = getJdbcDriverFolder()) {
  settings <- list(
    connectionDetails = NULL,
    cdmDatabaseSchema = NULL,
    vocabularyDatabaseSchema = NULL,
    cohortDatabaseSchema = NULL,
    tempEmulationSchema = NULL,
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )

  if (dbmsPlatform == "sqlite") {
    settings$connectionDetails <- Eunomia::getEunomiaConnectionDetails()
    settings$cdmDatabaseSchema <- "main"
    settings$vocabularyDatabaseSchema <- "main"
    settings$cohortDatabaseSchema <- "main"
    settings$cohortTable <- "cohort"
    settings$needsDrivers <- FALSE
    return(settings)
  }

  if (dbmsPlatform == "bigquery") {
    settings$needsWindowsOnly <- TRUE
    settings$connectionDetails <- getBigQueryConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
    settings$tempEmulationSchema <- Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "oracle") {
    settings$connectionDetails <- getOracleConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
    settings$tempEmulationSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "postgresql") {
    settings$connectionDetails <- getPostgresqlConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "redshift") {
    settings$connectionDetails <- getRedshiftConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "snowflake") {
    settings$connectionDetails <- getSnowflakeConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
    settings$tempEmulationSchema <- Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "spark") {
    settings$connectionDetails <- getSparkConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
    settings$tempEmulationSchema <- Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
    return(settings)
  }

  if (dbmsPlatform == "sql server") {
    settings$connectionDetails <- getSqlServerConnectionDetails(jdbcDriverFolder)
    settings$cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
    settings$vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
    settings$cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
    return(settings)
  }

  stop(sprintf("Unsupported DBMS '%s'.", dbmsPlatform), call. = FALSE)
}
