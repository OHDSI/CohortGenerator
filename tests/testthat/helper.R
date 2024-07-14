#' Create the Circe cohort expression from a JSON file for generating
#' SQL dynamically
#'
#' @description
#' This function constructs a Circe cohort expression from a JSON file for use
#' with other CirceR functions.
#'
#' @param filePath      The file path containing the Circe JSON file
#'
createCirceExpressionFromFile <- function(filePath) {
  cohortExpression <- readChar(filePath, file.info(filePath)$size)
  return(CirceR::cohortExpressionFromJson(cohortExpression))
}


generateSql <- function(cohortJsonFileName, generateStats = FALSE) {
  cohortExpression <- createCirceExpressionFromFile(cohortJsonFileName)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = generateStats))
  return(cohortSql)
}

# Used to add a SQL column to the "cohorts" data frame
# and toggle if inclusion stats are generated for the given SQL
# definition
getCohortsForTest <- function(cohorts, generateStats = FALSE) {
  cohortSql <- data.frame()
  for (i in 1:nrow(cohorts)) {
    cohortSql <- rbind(cohortSql, data.frame(sql = generateSql(cohorts$cohortJsonFile[i], generateStats)))
  }
  if (length(intersect(colnames(cohorts), c("sql"))) == 1) {
    cohorts$sql <- NULL
  }
  cohorts <- cbind(cohorts, cohortSql)
  return(cohorts)
}

# This will gather all of the cohort JSON in the package for use in the tests
cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
cohorts <- setNames(data.frame(matrix(ncol = 5, nrow = 0), stringsAsFactors = FALSE), c("atlasId", "cohortId", "cohortName", "json", "cohortJsonFile"))
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohorts <- rbind(cohorts, data.frame(
    atlasId = i,
    cohortId = i,
    cohortName = cohortFullName,
    json = cohortJson,
    cohortJsonFile = cohortJsonFileName,
    stringsAsFactors = FALSE
  ))
}

# Helper function
getNegativeControlOutcomeCohortsForTest <- function(setCohortIdToConceptId = TRUE) {
  negativeControlOutcomes <- readCsv(file = system.file("testdata/negativecontrols/negativeControlOutcomes.csv",
    package = "CohortGenerator",
    mustWork = TRUE
  ))
  if (setCohortIdToConceptId) {
    negativeControlOutcomes$cohortId <- negativeControlOutcomes$outcomeConceptId
  } else {
    negativeControlOutcomes$cohortId <- seq.int(nrow(negativeControlOutcomes))
  }
  invisible(negativeControlOutcomes)
}


getPlatformConnectionDetails <- function(dbmsPlatform) {
  # Get drivers for test platform
  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- "~/.jdbcDrivers"
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
  }

  options("sqlRenderTempEmulationSchema" = NULL)
  if (dbmsPlatform == "sqlite") {
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
    cdmDatabaseSchema <- "main"
    vocabularyDatabaseSchema <- "main"
    cohortDatabaseSchema <- "main"
    options("sqlRenderTempEmulationSchema" = NULL)
    cohortTable <- "cohort"
  } else {
    if (dbmsPlatform == "bigquery") {
      # To avoid rate limit on BigQuery, only test on 1 OS:
      if (.Platform$OS.type == "windows") {
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
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = dbmsPlatform,
          user = "",
          password = "",
          connectionString = !!bqConnectionString,
          pathToDriver = jdbcDriverFolder
        )
        cdmDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        vocabularyDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        cohortDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
        options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA"))
      } else {
        return(NULL)
      }
    } else if (dbmsPlatform == "oracle") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_ORACLE_USER"),
        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
        server = Sys.getenv("CDM5_ORACLE_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "postgresql") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "redshift") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_REDSHIFT_USER"),
        password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
        server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "snowflake") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM_SNOWFLAKE_USER"),
        password = URLdecode(Sys.getenv("CDM_SNOWFLAKE_PASSWORD")),
        connectionString = Sys.getenv("CDM_SNOWFLAKE_CONNECTION_STRING"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "spark") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_SPARK_USER"),
        password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD")),
        connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "sql server") {
      connectionDetails <- createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
    }

    # Add drivers
    DatabaseConnector::downloadJdbcDrivers(dbmsPlatform, pathToDriver = jdbcDriverFolder)
    # Table created to avoid collisions
    cohortTable <- paste0("ct_", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))
  }

  return(list(
    dbmsPlatform = dbmsPlatform,
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema
  ))
}
