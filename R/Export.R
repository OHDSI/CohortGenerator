# Copyright 2023 Observational Health Data Sciences and Informatics
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
#' call.
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
#' @export
exportCohortStatsTables <- function(connectionDetails,
                                    connection = NULL,
                                    cohortDatabaseSchema,
                                    cohortTableNames = getCohortTableNames(),
                                    cohortStatisticsFolder,
                                    snakeCaseToCamelCase = TRUE,
                                    fileNamesInSnakeCase = FALSE,
                                    incremental = FALSE,
                                    databaseId = NULL) {
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!dir.exists(cohortStatisticsFolder)) {
    dir.create(cohortStatisticsFolder, recursive = TRUE)
  }

  # Export the stats
  exportStats <- function(table,
                          fileName,
                          includeDatabaseId) {
    data <- getStatsTable(
      connection = connection,
      table = table,
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      databaseId = databaseId,
      cohortDatabaseSchema = cohortDatabaseSchema,
      includeDatabaseId = includeDatabaseId
    )

    fullFileName <- file.path(cohortStatisticsFolder, fileName)
    ParallelLogger::logInfo("- Saving data to - ", fullFileName)
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
    tableName = cohortTableNames$cohortInclusionTable,
    fileName = "cohort_inclusion.csv",
    includeDatabaseId = FALSE
  )
  tablesToExport <- rbind(tablesToExport, data.frame(
    tableName = cohortTableNames$cohortInclusionResultTable,
    fileName = "cohort_inc_result.csv",
    includeDatabaseId = TRUE
  ))
  tablesToExport <- rbind(tablesToExport, data.frame(
    tableName = cohortTableNames$cohortInclusionStatsTable,
    fileName = "cohort_inc_stats.csv",
    includeDatabaseId = TRUE
  ))
  tablesToExport <- rbind(tablesToExport, data.frame(
    tableName = cohortTableNames$cohortSummaryStatsTable,
    fileName = "cohort_summary_stats.csv",
    includeDatabaseId = TRUE
  ))
  tablesToExport <- rbind(tablesToExport, data.frame(
    tableName = cohortTableNames$cohortCensorStatsTable,
    fileName = "cohort_censor_stats.csv",
    includeDatabaseId = TRUE
  ))
  for (i in 1:nrow(tablesToExport)) {
    fileName <- ifelse(test = fileNamesInSnakeCase,
      yes = tablesToExport$fileName[i],
      no = SqlRender::snakeCaseToCamelCase(tablesToExport$fileName[i])
    )
    exportStats(
      table = tablesToExport$tableName[i],
      fileName = fileName,
      includeDatabaseId = tablesToExport$includeDatabaseId[i]
    )
  }
}
