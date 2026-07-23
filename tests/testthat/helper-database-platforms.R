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

getSnowflakeConnectionDetails <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  DatabaseConnector::createConnectionDetails(
    dbms = "snowflake",
    user = Sys.getenv("CDM_SNOWFLAKE_USER"),
    password = URLdecode(Sys.getenv("CDM_SNOWFLAKE_PASSWORD")),
    connectionString = Sys.getenv("CDM_SNOWFLAKE_CONNECTION_STRING"),
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

getSqliteDatabaseSettings <- function() {
  list(
    connectionDetails = Eunomia::getEunomiaConnectionDetails(),
    cdmDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    tempEmulationSchema = NULL,
    needsDrivers = FALSE,
    needsWindowsOnly = FALSE,
    cohortTable = "cohort"
  )
}

getPostgresqlDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getPostgresqlConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"),
    tempEmulationSchema = NULL,
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getOracleDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getOracleConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"),
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getRedshiftDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getRedshiftConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA"),
    tempEmulationSchema = NULL,
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getSparkDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getSparkConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM5_SPARK_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_SPARK_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA"),
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getSqlServerDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getSqlServerConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA"),
    tempEmulationSchema = NULL,
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getSnowflakeDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getSnowflakeConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA"),
    needsDrivers = TRUE,
    needsWindowsOnly = FALSE
  )
}

getBigQueryDatabaseSettings <- function(jdbcDriverFolder = getJdbcDriverFolder()) {
  list(
    connectionDetails = getBigQueryConnectionDetails(jdbcDriverFolder),
    cdmDatabaseSchema = Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA"),
    needsDrivers = TRUE,
    needsWindowsOnly = TRUE
  )
}

getDatabaseTestContext <- function(dbmsPlatform, jdbcDriverFolder = getJdbcDriverFolder()) {
  settings <- resolveDatabasePlatformSettings(dbmsPlatform, jdbcDriverFolder)

  list(
    dbmsPlatform = dbmsPlatform,
    connectionDetails = settings$connectionDetails,
    cohortDatabaseSchema = settings$cohortDatabaseSchema,
    cohortTable = if (isTRUE(settings$needsDrivers)) {
      paste0("ct_", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))
    } else {
      settings$cohortTable
    },
    cdmDatabaseSchema = settings$cdmDatabaseSchema,
    vocabularyDatabaseSchema = settings$vocabularyDatabaseSchema,
    tempEmulationSchema = settings$tempEmulationSchema,
    needsWindowsOnly = isTRUE(settings$needsWindowsOnly)
  )
}

isBigQuerySupportedOnCurrentPlatform <- function(dbmsPlatform) {
  !identical(dbmsPlatform, "bigquery") || .Platform$OS.type == "windows"
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
  if (dbmsPlatform == "sqlite") {
    return(getSqliteDatabaseSettings())
  }

  if (dbmsPlatform == "bigquery") {
    return(getBigQueryDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "oracle") {
    return(getOracleDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "postgresql") {
    return(getPostgresqlDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "redshift") {
    return(getRedshiftDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "snowflake") {
    return(getSnowflakeDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "spark") {
    return(getSparkDatabaseSettings(jdbcDriverFolder))
  }

  if (dbmsPlatform == "sql server") {
    return(getSqlServerDatabaseSettings(jdbcDriverFolder))
  }

  stop(sprintf("Unsupported DBMS '%s'.", dbmsPlatform), call. = FALSE)
}
