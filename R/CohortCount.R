#' Count the cohort(s)
#'
#' @description
#' Computes the subject and entry count per cohort
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds            The cohort Id(s) used to reference the cohort in the cohort
#'                             table. If left empty, all cohorts in the table will be included.
#'
#' @return
#' A data frame with cohort counts
#'
#' @export
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortCounts.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::render(sql = sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable,
                           cohort_ids = cohortIds)
  sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
  tablesInServer <- tolower(DatabaseConnector::getTableNames(conn = connection, databaseSchema = cohortDatabaseSchema))
  if (tolower(cohortTable) %in% tablesInServer) {
    counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Counting cohorts took", signif(delta, 3), attr(delta, "units")))
    return(counts)
  } else {
    warning('Cohort table was not found. Was it created?')
    return(NULL)
  }
}