# Copyright 2025 Observational Health Data Sciences and Informatics
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
#' @template minCellCount
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
                                    minCellCount = 5,
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
                          resultsDataModelTableName,
                          tablePrefix) {
    fullFileName <- file.path(cohortStatisticsFolder, paste0(tablePrefix, fileName))
    primaryKeyColumns <- getPrimaryKey(resultsDataModelTableName)
    columnsToCensor <- getColumnsToCensor(resultsDataModelTableName)
    rlang::inform(paste0("- Saving data to - ", fullFileName))

    # Make sure the data is censored before saving
    if (length(columnsToCensor) > 0) {
      for (i in seq_along(columnsToCensor)) {
        colName <- ifelse(isTRUE(snakeCaseToCamelCase), yes = columnsToCensor[i], no = SqlRender::camelCaseToSnakeCase(columnsToCensor[i]))
        data <- data %>%
          enforceMinCellValue(colName, minCellCount)
      }
    }

    if (incremental) {
      # Dynamically build the arguments to the saveIncremental
      # to specify the primary key(s) for the file
      args <- list(
        data = data,
        file = fullFileName
      )
      for (i in seq_along(primaryKeyColumns)) {
        colName <- ifelse(isTRUE(snakeCaseToCamelCase), yes = primaryKeyColumns[i], no = SqlRender::camelCaseToSnakeCase(primaryKeyColumns[i]))
        args[[colName]] <- data[[colName]]
      }
      do.call(
        what = CohortGenerator::saveIncremental,
        args = args
      )
    } else {
      .writeCsv(x = data, file = fullFileName)
    }
  }

  tablesToExport <- data.frame(
    tableName = c("cohortInclusionResultTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable", "cohortCensorStatsTable"),
    fileName = c("cohort_inc_result.csv", "cohort_inc_stats.csv", "cohort_summary_stats.csv", "cohort_censor_stats.csv"),
    resultsDataModelTableName = c("cg_cohort_inc_result", "cg_cohort_inc_stats", "cg_cohort_summary_stats", "cg_cohort_censor_stats")
  )

  if (is.null(cohortDefinitionSet)) {
    warning("No cohortDefinitionSet specified; please make sure you've inserted the inclusion rule names using the insertInclusionRuleNames function.")
    tablesToExport <- rbind(tablesToExport, data.frame(
      tableName = "cohortInclusionTable",
      fileName = "cohort_inclusion.csv",
      resultsDataModelTableName = "cg_cohort_inclusion"
    ))
  } else {
    inclusionRules <- getCohortInclusionRules(cohortDefinitionSet)
    names(inclusionRules) <- SqlRender::camelCaseToSnakeCase(names(inclusionRules))
    exportStats(
      data = inclusionRules,
      fileName = "cohort_inclusion.csv",
      resultsDataModelTableName = "cg_cohort_inclusion",
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
      resultsDataModelTableName = tablesToExport$resultsDataModelTableName[[i]],
      tablePrefix = tablePrefix
    )
  }
}


addSubsetColumns <- function(cohortDefinitionSet) {
  if (nrow(cohortDefinitionSet) > 0 & !hasSubsetDefinitions(cohortDefinitionSet)) {
    cohortDefinitionSet$isSubset <- FALSE
    cohortDefinitionSet$subsetDefinitionId <- NA
    cohortDefinitionSet$subsetParent <- cohortDefinitionSet$cohortId
  }

  return(cohortDefinitionSet)
}

exportCohortDefinitionSet <- function(outputFolder, cohortDefinitionSet = NULL) {
  cohortDefinitions <- createEmptyResult("cg_cohort_definition")
  cohortSubsets <- createEmptyResult("cg_cohort_subset_definition")
  if (!is.null(cohortDefinitionSet)) {
    cdsCohortSubsets <- getSubsetDefinitions(cohortDefinitionSet)
    if (length(cdsCohortSubsets) > 0) {
      for (i in seq_along(cdsCohortSubsets)) {
        cohortSubsets <- rbind(
          cohortSubsets,
          data.frame(
            subsetDefinitionId = cdsCohortSubsets[[i]]$definitionId,
            json = as.character(cdsCohortSubsets[[i]]$toJSON())
          )
        )
      }
    } else {
      cohortDefinitionSet <- cohortDefinitionSet |> addSubsetColumns()
    }
    # Massage and save the cohort definition set
    colsToRename <- c("cohortId", "cohortName", "sql", "json")
    colInd <- which(names(cohortDefinitionSet) %in% colsToRename)
    names(cohortDefinitionSet)[colInd] <- c("cohortDefinitionId", "cohortName", "sqlCommand", "json")
    if (!"description" %in% names(cohortDefinitionSet)) {
      cohortDefinitionSet$description <- ""
    }
    cohortDefinitions <- cohortDefinitionSet[, intersect(names(cohortDefinitions), names(cohortDefinitionSet))]
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
  result <- result[FALSE,]
  return(result)
}

getPrimaryKey <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(.data$tableName == !!tableName & tolower(.data$primaryKey) == "yes") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase()
  return(columns)
}

getColumnsToCensor <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(.data$tableName == !!tableName & tolower(.data$minCellCount) == "yes") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase()
  return(columns)
}

enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(pull(data, fieldName)) &
    pull(data, fieldName) < minValues &
    pull(data, fieldName) != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor) / nrow(data), 1)
    message(
      "    censoring ",
      sum(toCensor),
      " values (",
      percent,
      "%) from ",
      fieldName,
      " because value below minimum"
    )
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}
