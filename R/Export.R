# Copyright 2024 Observational Health Data Sciences and Informatics
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

#' Export the cohort statistics tables to the file system
#'
#' @description
#' This function retrieves the data from the cohort statistics tables and
#' writes them to the inclusion statistics folder specified in the function
#' call. NOTE: inclusion rule names are handled in one of two ways:
#' 
#' 1. You can specify the cohortDefinitionSet parameter and the inclusion rule 
#' names will be extracted from the data.frame. 
#' 2. You can insert the inclusion rule names into the database using the
#' insertInclusionRuleNames function of this package. 
#' 
#' The first approach is preferred as to avoid the warning emitted.
#'
#' @template Connection
#'
#' @template CohortTableNames
#'
#' @param cohortStatisticsFolder      The path to the folder where the cohort statistics folder
#'                                    where the results will be written
#'
#' @param snakeCaseToCamelCase        Should column names in the exported files
#'                                    convert from snake_case to camelCase? Default is FALSE
#'
#' @param fileNamesInSnakeCase        Should the exported files use snake_case? Default is FALSE
#'
#' @param incremental                 If \code{incremental = TRUE}, results are written to update values instead of
#'                                    overwriting an existing results
#'
#' @param databaseId                  Optional - when specified, the databaseId will be added
#'                                    to the exported results
#'                                    
#' @template CohortDefinitionSet
#' 
#' @param tablePrefix Optional - allows to append a prefix to the exported
#'                    file names.
#'
#' @export
exportCohortStatsTables <- function(connectionDetails,
                                    connection = NULL,
                                    cohortDatabaseSchema,
                                    cohortTableNames = getCohortTableNames(),
                                    cohortStatisticsFolder,
                                    snakeCaseToCamelCase = TRUE,
                                    fileNamesInSnakeCase = FALSE,
                                    incremental = FALSE,
                                    databaseId = NULL,
                                    cohortDefinitionSet = NULL,
                                    tablePrefix = "") {
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!dir.exists(cohortStatisticsFolder)) {
    dir.create(cohortStatisticsFolder, recursive = TRUE)
  }
  
  # Internal function to export the stats
  exportStats <- function(data,
                          fileName,
                          tablePrefix) {
    fullFileName <- file.path(cohortStatisticsFolder, paste0(tablePrefix, fileName))
    rlang::inform(paste0("- Saving data to - ", fullFileName))
    if (incremental) {
      if (snakeCaseToCamelCase) {
        cohortDefinitionIds <- unique(data$cohortDefinitionId)
        saveIncremental(data, fullFileName, cohortDefinitionId = cohortDefinitionIds)
      } else {
        cohortDefinitionIds <- unique(data$cohort_definition_id)
        saveIncremental(data, fullFileName, cohort_definition_id = cohortDefinitionIds)
      }
    } else {
      .writeCsv(x = data, file = fullFileName)
    }
  }
  
  tablesToExport <- data.frame(
    tableName = c("cohortInclusionResultTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable", "cohortCensorStatsTable"),
    fileName = c("cohort_inc_result.csv", "cohort_inc_stats.csv", "cohort_summary_stats.csv", "cohort_censor_stats.csv")
  )

  if (is.null(cohortDefinitionSet)) {
    warning("No cohortDefinitionSet specified; please make sure you've inserted the inclusion rule names using the insertInclusionRuleNames function.")
    tablesToExport <- rbind(tablesToExport, data.frame(
      tableName = "cohortInclusionTable",
      fileName = paste0(tablePrefix, "cohort_inclusion.csv")
    ))
  } else {
    inclusionRules <- getCohortInclusionRules(cohortDefinitionSet)
    names(inclusionRules) <- SqlRender::camelCaseToSnakeCase(names(inclusionRules))
    exportStats(
      data = inclusionRules,
      fileName = "cohort_inclusion.csv",
      tablePrefix = tablePrefix
    )
  }

  # Get the cohort statistics
  cohortStats <- getCohortStats(
    connectionDetails = connectionDetails,
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    databaseId = databaseId,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    cohortTableNames = cohortTableNames
  )
  
  for (i in 1:nrow(tablesToExport)) {
    fileName <- ifelse(test = fileNamesInSnakeCase,
      yes = tablesToExport$fileName[i],
      no = SqlRender::snakeCaseToCamelCase(tablesToExport$fileName[i])
    )
    exportStats(
      data = cohortStats[[tablesToExport$tableName[i]]],
      fileName = fileName,
      tablePrefix = tablePrefix
    )
  }
}

exportCohortDefinitionSet <- function(outputFolder, cohortDefinitionSet = NULL) {
  cohortDefinitions <- createEmptyResult("cg_cohort_definition")
  cohortSubsets <- createEmptyResult("cg_cohort_subset_definition")
  if (!is.null(cohortDefinitionSet)) {
    # Massage and save the cohort definition set
    colsToRename <- c("cohortId", "cohortName", "sql", "json")
    colInd <- which(names(cohortDefinitionSet) %in% colsToRename)
    cohortDefinitions <- cohortDefinitionSet
    names(cohortDefinitions)[colInd] <- c("cohortDefinitionId", "cohortName", "sqlCommand", "json")
    cohortDefinitions$description <- ""
    cdsCohortSubsets <- getSubsetDefinitions(cohortDefinitionSet)
    if (length(cdsCohortSubsets) > 0) {
      for (i in seq_along(cdsCohortSubsets)) {
        cohortSubsets <- rbind(cohortSubsets, 
                               data.frame(
                                 subsetDefinitionId = cdsCohortSubsets[[i]]$definitionId,
                                 json = as.character(cdsCohortSubsets[[i]]$toJSON())
                               ))
      }
    }
  }
  writeCsv(
    x = cohortDefinitions,
    file = file.path(outputFolder, "cg_cohort_definition.csv")
  )
  writeCsv(
    x = cohortSubsets,
    file = file.path(outputFolder, "cg_cohort_subset_definition.csv")
  )
}

createEmptyResult <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(.data$tableName == !!tableName) %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase()
  result <- vector(length = length(columns))
  names(result) <- columns
  result <- tibble::as_tibble(t(result), name_repair = "check_unique")
  result <- result[FALSE, ]
  return(result)
}