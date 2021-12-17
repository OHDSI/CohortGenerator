#' Create an empty cohort definition set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{generateCohortSet}.
#'
#' @return
#' Returns an empty cohort set data.frame
#' 
#' @export
createEmptyCohortDefinitionSet <- function() {
  return(setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("atlasId", "cohortId","cohortName", "sql")))
}

#' Get a cohort definition set embedded in a package
#'
#' @description
#' This function supports the legacy way of storing a cohort definition set in a package,
#' with a CSV file, JSON files, and SQL files in the `inst` folder.
#'
#' @param packageName The name of the package containing the cohort definitions.
#' @param fileName    The path to the CSV file containing the list of cohorts to create. 
#'
#' @return
#' Returns a cohort set data.frame
#' 
#' @export
getCohortDefinitionSetFromPackage <- function(packageName, fileName = "settings/CohortsToCreate.csv") {
  getPathInPackage <- function(fileName, package) {
    path <- system.file(fileName, package = packageName)
    if (path == "") {
      stop(sprintf("Cannot find '%s' in the %s package", fileName, packageName))
    } else {
      return(path)
    }
  }
  
  pathToCsv <- getPathInPackage(fileName, package = packageName)
  cohorts <- read.csv(pathToCsv)
  getSql<- function(name) {
    pathToSql <- getPathInPackage(file.path("sql", "sql_server", sprintf("%s.sql", name)), package = packageName)
    SqlRender::readSql(pathToSql)
  }
  sql <- sapply(cohorts$name, getSql)
  getJson<- function(name) {
    pathToJson <- getPathInPackage(file.path("cohorts", sprintf("%s.json", name)), package = packageName)
    SqlRender::readSql(pathToJson)
  }
  json <- sapply(cohorts$name, getJson)
  return(data.frame(cohortId = cohorts$cohortId,
                    cohortName = cohorts$cohortName, 
                    sql = sql,
                    json = json))
}

#' Get a cohort definition set
#'
#' @description
#' This function supports the legacy way of retrieving a cohort definition set 
#' from the file system or in a package. This function supports the legacy way of
#' storing a cohort definition set in a package with a CSV file, JSON files, 
#' and SQL files in the `inst` folder.
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
#' @param packageName The name of the package containing the cohort definitions.
#' 
#' @param warnOnMissingJson Provide a warning if a .JSON file is not found for a 
#'                          cohort in the settings file
#' 
#' @param verbose           When TRUE, logging messages are emitted to indicate export
#'                          progress.
#'
#' @return
#' Returns a cohort set data.frame
#' 
#' @export
getCohortDefinitionSet <- function(settingsFolder = "settings",
                                   settingsFileName = "CohortsToCreate.csv",
                                   jsonFolder = "cohorts",
                                   sqlFolder = "sql/sql_server",
                                   cohortFileNameFormat = "%s",
                                   cohortFileNameValue = c("cohortName"),
                                   packageName = NULL,
                                   warnOnMissingJson = TRUE,
                                   verbose = TRUE) {
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)
  
  getPath <- function(fileName, package = package, verbose = verbose) {
    if (!is.null(packageName)) {
      if (verbose) {
        ParallelLogger::logInfo(" --- Loading ", fileName, " from package: ", packageName)
      }
      # NOTE: mustWork = TRUE will cause this function to stop if the settingsFileName is not found
      # in the setting file
      return(system.file(fileName, package = packageName, mustWork = TRUE))
    } else {
      return(fileName)
    }
  }
  # Read the settings file which holds the cohortDefinitionSet
  settings <- readr::read_csv(getPath(fileName = file.path(settingsFolder, settingsFileName)),
                              col_types = readr::cols(), 
                              lazy = FALSE)
  
  checkmate::assert_true(all(cohortFileNameValue %in% names(settings)))
  
  readFile <- function(fileName, warnOnMissingJson = warnOnMissingJson, verbose = verbose) {
    if (verbose) {
      ParallelLogger::logInfo(" --- Loading: ", fileName)
    }
    # TODO: Do this operation in a tryCatch
    # so we can emit a warning if the JSON file is
    # not found on the file system and return NA.
    SqlRender::readSql(fileName)
  }

  # Read the JSON/SQL files
  fileData <- data.frame()
  for(i in 1:nrow(settings)) {
    cohortId <- settings$cohortId[i]
    cohortFileName <- .removeNonAsciiCharacters(cohortDefinitionSet$cohortName[i])
    fileNameRoot <- .getFileNameFromCohortDefinitionSet(cohortDefinitionSet = settings,
                                                        cohortFileNameFormat = cohortFileNameFormat)
    fileNameRoot <- .removeNonAsciiCharacters(fileNameRoot)
    json <- readFile(fileName = getPath(fileName = file.path(jsonFolder, paste0(fileNameRoot, ".json"))))
    sql <- readFile(fileName = getPath(fileName = file.path(sqlFolder, paste0(fileNameRoot, "sql"))))
    fileData <- rbind(fileData, data.frame(json = json,
                                           sql = sql))
  }
  
  if (all(is.null(settings$atlasId))) {
    settings$atlasId <- settings$cohortIdId
  }
  cohortDefinitionSet <- cbind(settings, fileData)
  invisible(cohortDefinitionSet)
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
  settingsColumns <- .getSettingsFileColumns()
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
  readr::write_csv(x =  cohortDefinitionSet[,settingsColumns], file = file.path(settingsFolder, settingsFileName))
  
  # Export the SQL & JSON for each entry
  for(i in 1:nrow(cohortDefinitionSet)) {
    cohortId <- cohortDefinitionSet$cohortId[i]
    cohortName <- .removeNonAsciiCharacters(cohortDefinitionSet$cohortName[i])
    json <- ifelse(is.na(cohortDefinitionSet$json[i]), cohortDefinitionSet$json[i], .removeNonAsciiCharacters(cohortDefinitionSet$json[i]))
    sql <- cohortDefinitionSet$sql[i]
    fileNameRoot <- .getFileNameFromCohortDefinitionSet(cohortDefinitionSetRow = cohortDefinitionSet[i,],
                                                        cohortFileNameValue = cohortFileNameValue,
                                                        cohortFileNameFormat = cohortFileNameFormat)
    if (verbose) {
      ParallelLogger::logInfo("Exporting (", i, "/", nrow(cohortDefinitionSet), "): ", cohortName)
    }
    if (!is.na(json)) {
      SqlRender::writeSql(sql = json, targetFile = file.path(jsonFolder, paste0(fileNameRoot, ".json")))
    }
    SqlRender::writeSql(sql = sql, targetFile = file.path(sqlFolder, paste0(fileNameRoot, ".sql")))
  }
  
  ParallelLogger::logInfo("Cohort definition saved")
}

.getSettingsFileColumns <- function() { 
  return(c("atlasId", "cohortId", "cohortName"))
}

.getFileNameFromCohortDefinitionSet <- function(cohortDefinitionSetRow,
                                                cohortFileNameValue,
                                                cohortFileNameFormat) {
  checkmate::assertDataFrame(cohortDefinitionSetRow, min.rows = 1, max.rows = 1, col.names = "named")
  # Create the list of arguments to pass to stri_sprintf
  # to create the file name
  argList <- list(format = cohortFileNameFormat)
  for(j in 1:length(cohortFileNameValue)) {
    argList <- append(argList, cohortDefinitionSetRow[1,cohortFileNameValue[j]][[1]])
  }
  fileNameRoot <- do.call(stringi::stri_sprintf, argList)
  return(fileNameRoot)
}

.removeNonAsciiCharacters <- function(expression) {
  return(stringi::stri_trans_general(expression, "latin-ascii"))
}
