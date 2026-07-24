# Copyright 2026 Observational Health Data Sciences and Informatics
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
#'                                    overwriting an existing results (deprecated)
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

    .writeCsv(x = data, file = fullFileName)
  }

  tablesToExport <- data.frame(
    tableName = c("cohortInclusionResultTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable", "cohortCensorStatsTable", "cohortAttritionTable"),
    fileName = c("cohort_inc_result.csv", "cohort_inc_stats.csv", "cohort_summary_stats.csv", "cohort_censor_stats.csv", "cohort_attrition.csv"),
    resultsDataModelTableName = c("cg_cohort_inc_result", "cg_cohort_inc_stats", "cg_cohort_summary_stats", "cg_cohort_censor_stats", "cg_cohort_attrition")
  )

  inclusionRules <- NULL
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
    cohortTableNames = cohortTableNames,
    inclusionRules = inclusionRules
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


#' Export cohort subset statistics tables to the file system
#'
#' @description
#' This function retrieves the data from the cohort subset statistics table and
#' writes them to the subset statistics folder specified in the function call.
#'
#' @template Connection
#' @template CohortTableNames
#'
#' @param cohortSubsetStatisticsFolder The path to the folder where the cohort subset statistics
#'                                     results will be written.
#' @param snakeCaseToCamelCase         Should column names in the exported files
#'                                     convert from snake_case to camelCase? Default is FALSE
#' @param fileNamesInSnakeCase         Should the exported files use snake_case? Default is FALSE
#' @param databaseId                   Optional - when specified, the databaseId will be added
#'                                     to the exported results
#' @template minCellCount
#' @param tablePrefix                  Optional - allows to append a prefix to the exported
#'                                     file names.
#'
#' @export
exportCohortSubsetStatsTables <- function(connectionDetails,
                                          connection = NULL,
                                          cohortDatabaseSchema,
                                          cohortTableNames = getCohortTableNames(),
                                          cohortSubsetStatisticsFolder,
                                          snakeCaseToCamelCase = TRUE,
                                          fileNamesInSnakeCase = FALSE,
                                          databaseId = NULL,
                                          minCellCount = 5,
                                          tablePrefix = "") {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!dir.exists(cohortSubsetStatisticsFolder)) {
    dir.create(cohortSubsetStatisticsFolder, recursive = TRUE)
  }

  exportStats <- function(data,
                          fileName,
                          resultsDataModelTableName,
                          tablePrefix) {
    fullFileName <- file.path(cohortSubsetStatisticsFolder, paste0(tablePrefix, fileName))
    columnsToCensor <- getColumnsToCensor(resultsDataModelTableName)
    rlang::inform(paste0("- Saving data to - ", fullFileName))

    if (length(columnsToCensor) > 0) {
      for (i in seq_along(columnsToCensor)) {
        colName <- ifelse(isTRUE(snakeCaseToCamelCase), yes = columnsToCensor[i], no = SqlRender::camelCaseToSnakeCase(columnsToCensor[i]))
        data <- data %>%
          enforceMinCellValue(colName, minCellCount)
      }
    }

    .writeCsv(x = data, file = fullFileName)
  }

  subsetAttrition <- getStatsTable(
    connectionDetails = connectionDetails,
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    table = cohortTableNames$cohortSubsetAttritionTable,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    includeDatabaseId = TRUE,
    databaseId = databaseId
  )

  fileName <- ifelse(test = fileNamesInSnakeCase,
    yes = "cohort_subset_attrition.csv",
    no = SqlRender::snakeCaseToCamelCase("cohort_subset_attrition.csv")
  )

  exportStats(
    data = subsetAttrition,
    fileName = fileName,
    resultsDataModelTableName = "cg_cohort_subset_attrition",
    tablePrefix = tablePrefix
  )
}

addSubsetColumns <- function(cohortDefinitionSet) {
  if (nrow(cohortDefinitionSet) > 0 & !hasSubsetDefinitions(cohortDefinitionSet)) {
    cohortDefinitionSet$isSubset <- 0
    cohortDefinitionSet$subsetDefinitionId <- NA
    cohortDefinitionSet$subsetParent <- cohortDefinitionSet$cohortId
  }

  return(cohortDefinitionSet)
}

addTemplateColumns <- function(cohortDefinitionSet) {
  if (nrow(cohortDefinitionSet) > 0 & !hasTemplateDefinitions(cohortDefinitionSet)) {
    cohortDefinitionSet$isTemplatedCohort <- 0
  }

  return(cohortDefinitionSet)
}

exportCohortDefinitionSet <- function(outputFolder, cohortDefinitionSet = NULL) {
  cohortDefinitions <- createEmptyResult("cg_cohort_definition")
  cohortSubsets <- createEmptyResult("cg_cohort_subset_definition")
  cohortSubsetOperators <- createEmptyResult("cg_cohort_subset_operator")
  cohortTemplates <- createEmptyResult("cg_cohort_template_definition")
  cohortTemplateLink <- createEmptyResult("cg_cohort_template_link")
  if (!is.null(cohortDefinitionSet)) {
    templateDefinitions <- getTemplateDefinitions(cohortDefinitionSet)
    if (length(templateDefinitions) > 0) {
      for (template in templateDefinitions) {
        row <- data.frame(
          templateDefinitionId = template$getChecksum(),
          json = template$toJson() |> as.character(),
          templateName = template$name,
          templateSql = template$templateSql
        )
        cohortTemplates <- dplyr::bind_rows(cohortTemplates, row)
        linkRows <- data.frame(
          templateDefinitionId = template$getChecksum(),
          cohortDefinitionId = template$references$cohortId
        )
        cohortTemplateLink <- dplyr::bind_rows(cohortTemplateLink, linkRows)
      }
      cohortDefinitionSet$isTemplatedCohort <- as.integer(cohortDefinitionSet$isTemplatedCohort)
    } else {
      cohortDefinitionSet <- cohortDefinitionSet |> addTemplateColumns()
    }

    cdsCohortSubsets <- getSubsetDefinitions(cohortDefinitionSet)
    if (length(cdsCohortSubsets) > 0) {
      for (i in seq_along(cdsCohortSubsets)) {
        cohortSubsets <- dplyr::bind_rows(
          cohortSubsets,
          data.frame(
            subsetDefinitionId = cdsCohortSubsets[[i]]$definitionId,
            json = as.character(cdsCohortSubsets[[i]]$toJSON())
          )
        )
      }
      subsetOperatorRows <- list()
      for (subsetDefinition in cdsCohortSubsets) {
        subsetDefinitionName <- subsetDefinition$name
        subsetDefinitionId <- subsetDefinition$definitionId
        for (i in seq_along(subsetDefinition$subsetOperators)) {
          operator <- subsetDefinition$subsetOperators[[i]]
          operatorList <- operator$toList()
          subsetOperatorRows[[length(subsetOperatorRows) + 1]] <- data.frame(
            subsetDefinitionId = subsetDefinitionId,
            operatorName = operatorList$name,
            operatorSequence = as.integer(i - 1),
            operatorType = operatorList$subsetType,
            definitionJson = as.character(operator$toJSON()),
            stringsAsFactors = FALSE
          )
        }
      }
      if (length(subsetOperatorRows) > 0) {
        cohortSubsetOperators <- dplyr::bind_rows(subsetOperatorRows)
      }
      cohortDefinitionSet$isSubset <- as.integer(cohortDefinitionSet$isSubset)
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
  writeCsv(
    x = cohortSubsetOperators,
    file = file.path(outputFolder, "cg_cohort_subset_operator.csv")
  )

  writeCsv(
    x = cohortTemplates,
    file = file.path(outputFolder, "cg_cohort_template_definition.csv")
  )

  writeCsv(
    x = cohortTemplateLink,
    file = file.path(outputFolder, "cg_cohort_template_link.csv")
  )
}

createEmptyResult <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) |>
    dplyr::filter(tableName == !!tableName)

  # Initialize an empty list to hold columns
  resultList <- list()

  # Loop through each column info to create a strongly typed empty column
  for (i in seq_len(nrow(columns))) {
    colName <- SqlRender::snakeCaseToCamelCase(columns$columnName[i])
    dataType <- columns$dataType[i]

    # Map data types to R types
    colValue <- switch(tolower(dataType),
      "bigint" = as.numeric(NA),
      "varchar" = as.character(NA),
      "text" = as.character(NA),
      "int" = as.integer(NA),
      "timestamp" = as.POSIXct(NA)
    )

    # Fallback when no data type is found
    if (is.null(colValue)) {
      warning(paste(colName, "has data type", tolower(dataType), "which was not converted."))
      colValue <- as.character(NA)
    }

    # Assign to list
    resultList[[colName]] <- colValue
  }

  # Convert list to tibble
  result <- tibble::as_tibble(resultList)

  # Ensure zero rows
  result <- result[FALSE, ]

  return(result)
}

getPrimaryKey <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(tableName == !!tableName & tolower(primaryKey) == "yes") %>%
    dplyr::pull(columnName) %>%
    SqlRender::snakeCaseToCamelCase()
  return(columns)
}

getColumnsToCensor <- function(tableName) {
  columns <- readCsv(
    file = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")
  ) %>%
    dplyr::filter(tableName == !!tableName & tolower(minCellCount) == "yes") %>%
    dplyr::pull(columnName) %>%
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

#' Safely unbox values with jsonlite::unbox
#' where values are of length 0 they are converted to NULL
#' Only use on vectors of length 1 or 0 or it will throw an error (as unbox does)
#' @noRd
safeUnbox <- function(x) {
  if (is.factor(x)) {
    x <- as.character(x)
  }

  if (length(x) == 0) {
    x <- NULL
  }

  return(jsonlite::unbox(x))
}
