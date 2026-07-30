skipIfLiveDatabaseTest()

test_that("database test config loads the full platform list", {
  config <- getDatabaseTestConfig()

  expect_equal(config$schemaVersion, 1)
  expect_equal(config$package, "CohortGenerator")
  expect_equal(config$databaseConnection, "all")
  expect_equal(
    vapply(config$platforms, function(platform) platform$dbms, character(1)),
    c("sqlite", "postgresql", "sql server", "oracle", "redshift", "spark", "bigquery", "snowflake", "iris")
  )
  expect_true(all(vapply(config$platforms, function(platform) isTRUE(platform$enabled), logical(1))))
})

test_that("selected DBMS is read from HADES_TEST_DBMS", {
  withr::with_envvar(
    c(HADES_TEST_DBMS = "PostgreSQL"),
    {
      expect_equal(getSelectedTestDbms(), "postgresql")
      expect_equal(getDatabasePlatformConfig()$dbms, "postgresql")
    }
  )
})

test_that("missing selected DBMS returns NULL", {
  withr::with_envvar(
    c(HADES_TEST_DBMS = ""),
    {
      expect_null(getDatabasePlatformConfig())
    }
  )
})

test_that("unknown selected DBMS fails fast", {
  expect_error(
    getDatabasePlatformConfig("foo"),
    "Database platform 'foo' is not declared by CohortGenerator."
  )
})

test_that("environment variables are mapped per platform", {
  expect_setequal(
    getRequiredDatabaseEnvironmentVariables("postgresql"),
    c(
      "CDM5_POSTGRESQL_USER",
      "CDM5_POSTGRESQL_PASSWORD",
      "CDM5_POSTGRESQL_SERVER",
      "CDM5_POSTGRESQL_CDM_SCHEMA",
      "CDM5_POSTGRESQL_OHDSI_SCHEMA"
    )
  )
  expect_setequal(
    getRequiredDatabaseEnvironmentVariables("sql server"),
    c(
      "CDM5_SQL_SERVER_USER",
      "CDM5_SQL_SERVER_PASSWORD",
      "CDM5_SQL_SERVER_SERVER",
      "CDM5_SQL_SERVER_CDM_SCHEMA",
      "CDM5_SQL_SERVER_OHDSI_SCHEMA"
    )
  )
  expect_setequal(
    getRequiredDatabaseEnvironmentVariables("iris"),
    c(
      "CDM_IRIS_USER",
      "CDM_IRIS_PASSWORD",
      "CDM_IRIS_CONNECTION_STRING",
      "CDM_IRIS_CDM_SCHEMA",
      "CDM_IRIS_OHDSI_SCHEMA"
    )
  )
})

test_that("Spark connection strings are normalized to disable Arrow when missing", {
  expect_equal(
    normalizeSparkConnectionString(
      "jdbc:databricks://server:443/db;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/68446dd1e8d16d66;"
    ),
    "jdbc:databricks://server:443/db;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/68446dd1e8d16d66;EnableArrow=0"
  )
  expect_equal(
    normalizeSparkConnectionString(
      "jdbc:databricks://server:443/db;transportMode=http;ssl=1;AuthMech=3;EnableArrow=0;httpPath=/sql/1.0/warehouses/68446dd1e8d16d66;"
    ),
    "jdbc:databricks://server:443/db;transportMode=http;ssl=1;AuthMech=3;EnableArrow=0;httpPath=/sql/1.0/warehouses/68446dd1e8d16d66;"
  )
})

test_that("Snowflake connection strings are normalized to request JSON results when missing", {
  expect_equal(
    normalizeSnowflakeConnectionString(
      "jdbc:snowflake://server.snowflakecomputing.com?db=db&warehouse=wh"
    ),
    "jdbc:snowflake://server.snowflakecomputing.com?db=db&warehouse=wh&JDBC_QUERY_RESULT_FORMAT=JSON"
  )
  expect_equal(
    normalizeSnowflakeConnectionString(
      "jdbc:snowflake://server.snowflakecomputing.com?db=db&warehouse=wh&JDBC_QUERY_RESULT_FORMAT=JSON"
    ),
    "jdbc:snowflake://server.snowflakecomputing.com?db=db&warehouse=wh&JDBC_QUERY_RESULT_FORMAT=JSON"
  )
})

test_that("validateDatabaseTestEnvironment reports missing variables locally", {
  withr::with_envvar(
    c(
      CDM5_POSTGRESQL_USER = "",
      CDM5_POSTGRESQL_PASSWORD = "",
      CDM5_POSTGRESQL_SERVER = "",
      CDM5_POSTGRESQL_CDM_SCHEMA = "",
      CDM5_POSTGRESQL_OHDSI_SCHEMA = "",
      HADES_DATABASE_TEST = "false"
    ),
    {
      cond <- tryCatch(
        validateDatabaseTestEnvironment("postgresql"),
        condition = function(e) e
      )

      expect_s3_class(cond, "skip")
      expect_match(cond$message, "Missing environment variables for postgresql:")
    }
  )
})

test_that("validateDatabaseTestEnvironment fails missing variables in CI", {
  withr::with_envvar(
    c(
      CDM5_POSTGRESQL_USER = "",
      CDM5_POSTGRESQL_PASSWORD = "",
      CDM5_POSTGRESQL_SERVER = "",
      CDM5_POSTGRESQL_CDM_SCHEMA = "",
      CDM5_POSTGRESQL_OHDSI_SCHEMA = "",
      HADES_DATABASE_TEST = "true"
    ),
    {
      expect_error(
        validateDatabaseTestEnvironment("postgresql"),
        "Missing environment variables for postgresql:"
      )
    }
  )
})

test_that("BigQuery is only supported on Windows", {
  expect_false(isBigQuerySupportedOnCurrentPlatform("bigquery") && .Platform$OS.type != "windows")
  expect_true(isBigQuerySupportedOnCurrentPlatform("postgresql"))
})

test_that("database test context is assembled from resolved settings", {
  oldResolveDatabasePlatformSettings <- resolveDatabasePlatformSettings
  resolveDatabasePlatformSettings <<- function(dbmsPlatform, jdbcDriverFolder = getJdbcDriverFolder()) {
    list(
      connectionDetails = list(dbms = dbmsPlatform),
      cdmDatabaseSchema = "cdm_schema",
      vocabularyDatabaseSchema = "vocab_schema",
      cohortDatabaseSchema = "cohort_schema",
      tempEmulationSchema = "temp_schema",
      needsDrivers = TRUE,
      needsWindowsOnly = TRUE
    )
  }
  on.exit(
    resolveDatabasePlatformSettings <<- oldResolveDatabasePlatformSettings,
    add = TRUE
  )

  ctx <- getDatabaseTestContext("postgresql", jdbcDriverFolder = "C:/tmp")

  expect_equal(ctx$dbmsPlatform, "postgresql")
  expect_match(ctx$cohortTable, "^ct_")
  expect_equal(ctx$cdmDatabaseSchema, "cdm_schema")
  expect_equal(ctx$vocabularyDatabaseSchema, "vocab_schema")
  expect_equal(ctx$cohortDatabaseSchema, "cohort_schema")
  expect_equal(ctx$tempEmulationSchema, "temp_schema")
  expect_true(isTRUE(ctx$needsWindowsOnly))
})

test_that("platform connection details are assembled from settings", {
  settings <- list(
    connectionDetails = list(dbms = "postgresql"),
    cohortDatabaseSchema = "cohort_schema",
    cohortTable = "ct_123",
    cdmDatabaseSchema = "cdm_schema",
    vocabularyDatabaseSchema = "vocab_schema"
  )

  details <- assemblePlatformConnectionDetails("postgresql", settings)

  expect_equal(details$dbmsPlatform, "postgresql")
  expect_equal(details$connectionDetails$dbms, "postgresql")
  expect_equal(details$cohortDatabaseSchema, "cohort_schema")
  expect_equal(details$cohortTable, "ct_123")
  expect_equal(details$cdmDatabaseSchema, "cdm_schema")
  expect_equal(details$vocabularyDatabaseSchema, "vocab_schema")
})
