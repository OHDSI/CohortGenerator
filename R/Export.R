#' Export the cohort statistics tables to the file system
#'
#' @description
#' This function retrieves the data from the cohort statistics tables and
#' writes them to the inclusion statistics folder specified in the function
#' call. 
#'
#' @template Connection
#' 
#' @template CohortTableNames
#'                                    
#' @param cohortStatisticsFolder      The path to the folder where the cohort statistics folder 
#'                                    where the results will be written
#'                                    
#' @param incremental                 If \code{incremental = TRUE}, results are written to update values instead of 
#'                                    overwriting an existing results
#'                                    
#' @export
exportCohortStatsTables <- function(connectionDetails,
                                    connection = NULL,
                                    cohortDatabaseSchema,
                                    cohortTableNames = getCohortTableNames(),
                                    cohortStatisticsFolder,
                                    incremental = FALSE) {
  
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!file.exists(cohortStatisticsFolder)) {
    dir.create(cohortStatisticsFolder, recursive = TRUE)
  }  

  # Export the stats
  exportStats <- function(table, fileName) {
    ParallelLogger::logInfo("- Fetching data from ", table)
    sql <- "SELECT * FROM @cohort_database_schema.@table"
    data <- DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      snakeCaseToCamelCase = TRUE,
      table = table,
      cohort_database_schema = cohortDatabaseSchema
    )
    fullFileName <- file.path(cohortStatisticsFolder, fileName)
    if (incremental) {
      cohortIds <- unique(data$cohortDefinitionId)
      saveIncremental(data, fullFileName, cohortId = cohortIds)
    } else {
      readr::write_csv(x = data, file = fullFileName)
    }
  }
  exportStats(cohortTableNames$cohortInclusionTable, "cohortInclusion.csv")
  exportStats(cohortTableNames$cohortInclusionResultTable, "cohortIncResult.csv")
  exportStats(cohortTableNames$cohortInclusionStatsTable, "cohortIncStats.csv")
  exportStats(cohortTableNames$cohortSummaryStatsTable, "cohortSummaryStats.csv")
  exportStats(cohortTableNames$cohortCensorStatsTable, "cohortCensorStats.csv")
}