#' Export the cohort statistics tables to the file system
#'
#' @description
#' This function retrieves the data from the cohort statistics tables and
#' writes them to the inclusion statistics folder specified in the function
#' call. 
#'
#' @template Connection
#' 
#' @param cohortDatabaseSchema        The schema to hold the cohort tables. Note that for
#'                                    SQL Server, this should include both the database and schema
#'                                    name, for example 'scratch.dbo'.
#'
#' @param cohortTableNames            The names of the cohort statistics tables. See \code{\link{getCohortTableNames}}
#'                                    for more details.
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

#' Save the cohort definition set to the file system
#'
#' @description
#' This function saves a cohortDefinitionSet to the file system and provides
#' options for specifying where to write the individual elements: the settings
#' file will contain the cohort information as a CSV specified by the 
#' settingsFileName, the cohort JSON is written to the jsonFolder and the SQL
#' is written to the sqlFolder. We also provide a way to specify the 
#' json/sql file name format using the cohortFileNameFormat and 
#' cohortFileNameValue parameters.
#'
#' @template CohortDefinitionSet
#'
#' @param settingsFolder   The name of the folder that will hold the settingsFileName
#' 
#' @param settingsFileName The name of the CSV file that will hold the cohort information
#'                         including the atlasId, cohortId and cohortName
#'                  
#' @param jsonFolder       The name of the folder that will hold the JSON representation
#'                         of the cohort if it is available in the cohortDefinitionSet
#'                         
#' @param sqlFolder        The name of the folder that will hold the SQL representation
#'                         of the cohort.
#'                         
#' @param cohortFileNameFormat  Defines the format string  for naming the cohort 
#'                              JSON and SQL files. The format string follows the 
#'                              standard defined in the base sprintf function.
#'                              
#' @param cohortFileNameValue   Defines the columns in the cohortDefinitionSet to use
#'                              in conjunction with the cohortFileNameFormat parameter.
#'                              
#' @param verbose           When TRUE, logging messages are emitted to indicate export
#'                          progress.
#'                                    
#' @export
saveCohortDefinitionSet <- function(cohortDefinitionSet,
                                    settingsFolder = "inst/settings",
                                    settingsFileName = "CohortsToCreate.csv",
                                    jsonFolder = "inst/cohorts",
                                    sqlFolder = "inst/sql/sql_server",
                                    cohortFileNameFormat = "%s",
                                    cohortFileNameValue = c("cohortName"),
                                    verbose = TRUE) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)
  checkmate::assert_true(all(cohortFileNameValue %in% names(cohortDefinitionSet)))
  if (!file.exists(settingsFolder)) {
    dir.create(settingsFolder, recursive = TRUE)
  }
  if (!file.exists(jsonFolder)) {
    dir.create(jsonFolder, recursive = TRUE)
  }
  if (!file.exists(sqlFolder)) {
    dir.create(sqlFolder, recursive = TRUE)
  }
  
  # Export the cohortDefinitionSet to the settings folder
  if (verbose) {
    ParallelLogger::logInfo("Exporting cohortDefinitionSet to ", settingsFolder)
  }
  readr::write_csv(x =  cohortDefinitionSet[,c("atlasId", "cohortId", "cohortName")], file = file.path(settingsFolder, settingsFileName))
  
  # Export the SQL & JSON for each entry
  for(i in 1:nrow(cohortDefinitionSet)) {
    cohortId <- cohortDefinitionSet$cohortId[i]
    cohortName <- .removeNonAsciiCharacters(cohortDefinitionSet$cohortName[i])
    json <- ifelse(is.na(cohortDefinitionSet$json[i]), cohortDefinitionSet$json[i], .removeNonAsciiCharacters(cohortDefinitionSet$json[i]))
    sql <- cohortDefinitionSet$sql[i]
    # Create the list of arguments to pass to stri_sprintf
    # to create the file name
    argList <- list(format = cohortFileNameFormat)
    for(j in 1:length(cohortFileNameValue)) {
      argList <- append(argList, cohortDefinitionSet[i,cohortFileNameValue[j]][[1]])
    }
    fileNameRoot <- do.call(stringi::stri_sprintf, argList)
    if (verbose) {
      ParallelLogger::logInfo("Exporting (", i, "/", nrow(cohortDefinitionSet), "): ", cohortName)
    }
    if (!is.na(json)) {
      SqlRender::writeSql(sql = json, targetFile = file.path(jsonFolder, paste0(fileNameRoot, ".json")))
    }
    SqlRender::writeSql(sql = sql, targetFile = file.path(sqlFolder, paste0(fileNameRoot, ".sql")))
  }
  
  ParallelLogger::logInfo("Export complete")
}

.removeNonAsciiCharacters <- function(expression) {
  return(stringi::stri_trans_general(expression, "latin-ascii"))
}
