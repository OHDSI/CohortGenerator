# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of CohortGenerator
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
  return(setNames(data.frame(matrix(ncol = 3, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortName", "sql")))
}

#' Get a cohort definition set
#'
#' @description
#' This function supports the legacy way of retrieving a cohort definition set 
#' from the file system or in a package. This function supports the legacy way of
#' storing a cohort definition set in a package with a CSV file, JSON files, 
#' and SQL files in the `inst` folder.
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
#' @param verbose           When TRUE, extra logging messages are emitted
#'
#' @return
#' Returns a cohort set data.frame
#' 
#' @export
getCohortDefinitionSet <- function(settingsFileName = "settings/CohortsToCreate.csv",
                                   jsonFolder = "cohorts",
                                   sqlFolder = "sql/sql_server",
                                   cohortFileNameFormat = "%s",
                                   cohortFileNameValue = c("cohortName"),
                                   packageName = NULL,
                                   warnOnMissingJson = TRUE,
                                   verbose = FALSE) {
  
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)
  
  getPath <- function(fileName) {
    path = fileName
    if (!is.null(packageName)) {
      path = system.file(fileName, package = packageName)
    }
    if (verbose) {
      ParallelLogger::logInfo(paste0(" -- Loading ", basename(fileName), " from ", path))
    }
    if (!file.exists(path)) {
      if (grepl(".json$", tolower(basename(fileName))) && warnOnMissingJson) {
        errorMsg <- ifelse(is.null(packageName), 
                           paste0("File not found: ", path), 
                           paste0("File, ", fileName, " not found in package: ", packageName))
        warning(errorMsg)
      }
    }
    return(path)
  }
  
  # Read the settings file which holds the cohortDefinitionSet
  ParallelLogger::logInfo("Loading cohortDefinitionSet")
  settings <- readr::read_csv(getPath(fileName = settingsFileName),
                              col_types = readr::cols(), 
                              lazy = FALSE)
  
  settingsColumns <- .getSettingsFileRequiredColumns()
  checkmate::assert_true(all(settingsColumns %in% names(settings)))
  checkmate::assert_true(all(cohortFileNameValue %in% names(settings)))
  checkmate::assert_true((!all(.getFileDataColumns() %in% names(settings))))
  
  readFile <- function(fileName) {
    if (file.exists(fileName)) {
      return(SqlRender::readSql(fileName))
    } else {
      if (grepl(".json$", tolower(basename(fileName))) && warnOnMissingJson) {
        warning(paste0(" --- ", fileName, " not found"))
        return(NA)
      } else {
        stop(paste0("File not found: ", fileName))
      }
    }
  }

  # Read the JSON/SQL files
  fileData <- data.frame()
  for(i in 1:nrow(settings)) {
    cohortFileNameRoot <- .getFileNameFromCohortDefinitionSet(cohortDefinitionSetRow = settings[i,],
                                                              cohortFileNameValue = cohortFileNameValue,
                                                              cohortFileNameFormat = cohortFileNameFormat)
    cohortFileNameRoot <- .removeNonAsciiCharacters(cohortFileNameRoot)
    json <- readFile(fileName = getPath(fileName = file.path(jsonFolder, paste0(cohortFileNameRoot, ".json"))))
    sql <- readFile(fileName = getPath(fileName = file.path(sqlFolder, paste0(cohortFileNameRoot, ".sql"))))
    fileData <- rbind(fileData, data.frame(json = json,
                                           sql = sql))
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
#' @param settingsFileName The name of the CSV file that will hold the cohort information
#'                         including the cohortId and cohortName
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
                                    settingsFileName = "inst/settings/CohortsToCreate.csv",
                                    jsonFolder = "inst/cohorts",
                                    sqlFolder = "inst/sql/sql_server",
                                    cohortFileNameFormat = "%s",
                                    cohortFileNameValue = c("cohortName"),
                                    verbose = FALSE) {
  settingsColumns <- .getSettingsFileRequiredColumns()
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)
  checkmate::assert_true(all(settingsColumns %in% names(cohortDefinitionSet)))
  checkmate::assert_true(all(cohortFileNameValue %in% names(cohortDefinitionSet)))
  settingsFolder <- dirname(settingsFileName)
  if (!dir.exists(settingsFolder)) {
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
    ParallelLogger::logInfo("Exporting cohortDefinitionSet to ", settingsFileName)
  }
  # Write the settings file and ensure that the "sql" and "json" columns are
  # not included
  readr::write_csv(x =  cohortDefinitionSet[,-which(names(cohortDefinitionSet) %in% .getFileDataColumns())], file = settingsFileName)
  
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

.getSettingsFileRequiredColumns <- function() { 
  return(c("cohortId", "cohortName"))
}

.getFileDataColumns <- function() {
  return(c("json", "sql"))
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
