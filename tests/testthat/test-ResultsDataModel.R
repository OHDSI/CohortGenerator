library(CohortGenerator)
library(testthat)


getPostgresInfo <- function() {
  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- "~/.jdbcDrivers"
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
    DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
    withr::defer(
      {
        unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
      },
      testthat::teardown_env()
    )
  }

  postgresConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder
  )

  postgresResultsDatabaseSchema <- paste0("r", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))
  return(
    list(
      connectionDetails = postgresConnectionDetails,
      resultsSchema = postgresResultsDatabaseSchema
    )
  )
}

getSqliteInfo <- function() {
  databaseFile <- tempfile(fileext = ".sqlite")
  sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = databaseFile
  )
  sqliteResultsDatabaseSchema <- "main"
  return(
    list(
      connectionDetails = sqliteConnectionDetails,
      resultsSchema = sqliteResultsDatabaseSchema
    )
  )
}


testCreateSchema <- function(connectionDetails, resultsDatabaseSchema) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  if (connectionDetails$dbms != "sqlite") {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(
      sql = sql,
      resultsDatabaseSchema = resultsDatabaseSchema,
      connection = connection
    )
  }
  suppressWarnings(
    createResultsDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = resultsDatabaseSchema,
    )
  )
  specifications <- getResultsDataModelSpecifications()
  for (tableName in unique(specifications$tableName)) {
    expect_true(DatabaseConnector::existsTable(
      connection = connection,
      databaseSchema = resultsDatabaseSchema,
      tableName = tableName
    ))
  }
  # Bad schema name
  expect_error(createResultsDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = "non_existant_schema"
  ))
}

testUploadResults <- function(connectionDetails, resultsDatabaseSchema, resultsFolder) {
  uploadResults(
    connectionDetails = connectionDetails,
    schema = resultsDatabaseSchema,
    resultsFolder = resultsFolder,
    purgeSiteDataBeforeUploading = FALSE
  )

  # Check if there's data:
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  specifications <- getResultsDataModelSpecifications()
  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
        primaryKey == "Yes") %>%
      dplyr::select(columnName) %>%
      dplyr::pull()

    if ("database_id" %in% primaryKey) {
      sql <- "SELECT COUNT(*) FROM @database_schema.@table_name WHERE database_id = '@database_id';"
      databaseIdCount <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        database_schema = resultsDatabaseSchema,
        table_name = tableName,
        database_id = "Eunomia"
      )[, 1]
      expect_true(databaseIdCount >= 0)
    }
  }
}

test_that("Create schema and upload on Postgres", {
  skip_on_cran()
  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempdir())
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)

  zip::unzip(
    zipfile = system.file(
      "testdata/Results_Eunomia.zip",
      package = "CohortGenerator"
    ),
    exdir = unzipFolder
  )

  postgresInfo <- getPostgresInfo()
  testCreateSchema(
    connectionDetails = postgresInfo$connectionDetails,
    resultsDatabaseSchema = postgresInfo$resultsSchema
  )

  testUploadResults(
    connectionDetails = postgresInfo$connectionDetails,
    resultsDatabaseSchema = postgresInfo$resultsSchema,
    resultsFolder = unzipFolder
  )


  on.exit(
    {
      connection <- DatabaseConnector::connect(connectionDetails = postgresInfo$connectionDetails)
      sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
      DatabaseConnector::renderTranslateExecuteSql(
        sql = sql,
        resultsDatabaseSchema = postgresInfo$resultsSchema,
        connection = connection
      )
      DatabaseConnector::disconnect(connection)
    },
    add = TRUE
  )
})

test_that("Create schema and upload on Sqlite", {
  skip_on_cran()
  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempdir())
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)

  zip::unzip(
    zipfile = system.file(
      "testdata/Results_Eunomia.zip",
      package = "CohortGenerator"
    ),
    exdir = unzipFolder
  )

  sqliteInfo <- getSqliteInfo()
  testCreateSchema(
    connectionDetails = sqliteInfo$connectionDetails,
    resultsDatabaseSchema = sqliteInfo$resultsSchema
  )

  testUploadResults(
    connectionDetails = sqliteInfo$connectionDetails,
    resultsDatabaseSchema = sqliteInfo$resultsSchema,
    resultsFolder = unzipFolder
  )

  on.exit(unlink(sqliteInfo$connectionDetails$server(), force = TRUE), add = TRUE)
})
