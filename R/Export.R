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


#' Extract Unique Concept Sets and Map to Cohorts
#'
#' @description
#' Parses a cohort definition set, extracts all unique concept sets and their mappings to cohorts, and writes them in RDBMS-normalized CSV files for downstream analysis or ETL.
#' Concept sets are uniquely identified via checksum hashes. Subset cohorts and "templated" cohorts are handled distinctly.
#'
#' @param cohortDefinitionSet Data frame or list. A cohort definition set, such as returned by `getCohortDefinitionSet()`.
#' @param conceptSetExportPath A path to export the files to
#'
#' @details
#' - Unique concept sets (from JSON or templated cohorts) are extracted and assigned hash-based IDs.
#' - Mappings between cohorts and concept sets, as well as concept set name associations, are recorded.
#' - Three CSV files are produced in `conceptSetExportPath`:
#'   - `cg_concept_set.csv`: Each row is a concept within a unique concept set.
#'   - `cg_cohort_concept_set.csv`: Maps concept sets to cohorts.
#'   - `cg_concept_set_name.csv`: Maps concept set names to their hash keys.
#'
#' Files are overwritten on each run.
#' Subsets are supported; concept sets from parent cohorts propagate as needed.
#'
#' @return Invisible `NULL`. Used for its side effects (CSV file export).
#'
#' @examples
#' \dontrun{
#' extractConceptSets()
#' }
#' @internal
#' @seealso [getCohortDefinitionSet()]
exportConceptSets <- function(cohortDefinitionSet, conceptSetExportPath) {
  # Named lists instead of fastmap
  conceptSets <- list()
  cohortChecksums <- list()
  cohortConceptSetMap <- list()
  conceptSetNames <- list()

  # --- Process non-subset cohorts ---
  nonSubset <- cohortDefinitionSet[!cohortDefinitionSet$isSubset, ]

  for (i in seq_len(nrow(nonSubset))) {
    cohortName <- nonSubset$cohortName[i]
    cohortId <- nonSubset$cohortId[i]
    isTemplatedCohort <- nonSubset$isTemplatedCohort[i]
    json <- nonSubset$json[i]

    if (isTemplatedCohort) {
      # Templated cohort — generate concept set directly
      conceptSet <- data.frame(
        conceptId = as.integer(cohortId / 1000),
        includeMapped = 0,
        isExcluded = 0,
        includeDescendants = 1
      )
      checksum <- getConceptSetChecksum(conceptSet)
      conceptSets[[checksum]] <- conceptSet

      # Update cohortConceptSetMap
      if (is.null(cohortConceptSetMap[[checksum]])) cohortConceptSetMap[[checksum]] <- integer()
      cohortConceptSetMap[[checksum]] <- sort(unique(c(cohortConceptSetMap[[checksum]], cohortId)))

      # Update cohortChecksums
      keyId <- as.character(cohortId)
      if (is.null(cohortChecksums[[keyId]])) cohortChecksums[[keyId]] <- character()
      cohortChecksums[[keyId]] <- sort(unique(c(cohortChecksums[[keyId]], checksum)))

      # Update names
      if (is.null(conceptSetNames[[checksum]])) conceptSetNames[[checksum]] <- character()
      conceptSetNames[[checksum]] <- unique(c(conceptSetNames[[checksum]], cohortName))

    } else {
      # Non-templated — parse JSON and extract codesets
      cohortDef <- jsonlite::fromJSON(json, simplifyDataFrame = FALSE)
      codesets <- extractCirceConceptSets(cohortDef)

      for (csname in names(codesets)) {
        if (!nrow(codesets[[csname]])) next

        conceptSet <- dplyr::select(
          codesets[[csname]],
          "conceptId", "includeMapped", "includeDescendants", "isExcluded"
        )
        checksum <- getConceptSetChecksum(conceptSet)
        conceptSets[[checksum]] <- conceptSet

        # Map concept set to cohorts
        if (is.null(cohortConceptSetMap[[checksum]])) cohortConceptSetMap[[checksum]] <- integer()
        cohortConceptSetMap[[checksum]] <- sort(unique(c(cohortConceptSetMap[[checksum]], cohortId)))

        # Map cohort to checksums
        keyId <- as.character(cohortId)
        if (is.null(cohortChecksums[[keyId]])) cohortChecksums[[keyId]] <- character()
        cohortChecksums[[keyId]] <- sort(unique(c(cohortChecksums[[keyId]], checksum)))

        # Map concept set name to ID
        if (is.null(conceptSetNames[[checksum]])) conceptSetNames[[checksum]] <- character()
        conceptSetNames[[checksum]] <- unique(c(conceptSetNames[[checksum]], csname))
      }
    }
  }

  # --- Add subset cohorts ---
  subsetRows <- cohortDefinitionSet[cohortDefinitionSet$isSubset, ]

  for (i in seq_len(nrow(subsetRows))) {
    cohortId <- subsetRows$cohortId[i]
    keyId <- as.character(cohortId)
    checksums <- cohortChecksums[[keyId]]

    if (!is.null(checksums) && length(checksums) > 0) {
      for (checksum in checksums) {
        if (is.null(cohortConceptSetMap[[checksum]])) cohortConceptSetMap[[checksum]] <- integer()
        cohortConceptSetMap[[checksum]] <- sort(unique(c(cohortConceptSetMap[[checksum]], cohortId)))
      }
    }
  }

  # --- Create export directory ---
  dir.create(conceptSetExportPath, showWarnings = FALSE)
  conceptSetFile <- file.path(conceptSetExportPath, "cg_concept_set.csv")
  cohortConceptSetFile <- file.path(conceptSetExportPath, "cg_cohort_concept_set.csv")
  conceptSetNamesFile <- file.path(conceptSetExportPath, "cg_concept_set_name.csv")

  unlink(c(conceptSetFile, cohortConceptSetFile, conceptSetNamesFile))

  # --- Export unique concept sets ---
  for (key in names(conceptSets)) {
    rows <- conceptSets[[key]]
    rows$conceptSetId <- key
    writeCsv(
      x = rows,
      file = conceptSetFile,
      append = file.exists(conceptSetFile)
    )
  }

  # --- Export mapping: concept set → cohorts ---
  for (key in names(cohortConceptSetMap)) {
    cohortIds <- cohortConceptSetMap[[key]]
    rows <- data.frame(conceptSetId = key, cohortDefinitionId = cohortIds)
    writeCsv(
      x = rows,
      file = cohortConceptSetFile,
      append = file.exists(cohortConceptSetFile)
    )
  }

  # --- Export mapping: names → IDs ---
  for (key in names(conceptSetNames)) {
    namesVec <- conceptSetNames[[key]]
    rows <- data.frame(conceptSetName = namesVec, conceptSetId = key)
    writeCsv(
      x = rows,
      file = conceptSetNamesFile,
      append = file.exists(conceptSetNamesFile)
    )
  }

  invisible(NULL)
}

exportCohortDefinitionSet <- function(outputFolder, cohortDefinitionSet = NULL) {
  cohortDefinitions <- createEmptyResult("cg_cohort_definition")
  cohortSubsets <- createEmptyResult("cg_cohort_subset_definition")
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
        linkRows <- data.frame(templateDefinitionId = template$getChecksum(),
                               cohortDefinitionId = template$references$cohortId)
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
      cohortDefinitionSet$isSubset <- as.integer(cohortDefinitionSet$isSubset)
    } else {
      cohortDefinitionSet <- cohortDefinitionSet |> addSubsetColumns()
    }

    exportConceptSets(cohortDefinitionSet, outputFolder)

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
    dplyr::filter(.data$tableName == !!tableName)

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
                       "timestamp" = as.POSIXct(NA))

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
