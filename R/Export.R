# Copyright 2022 Observational Health Data Sciences and Informatics
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
                                    incremental = FALSE,
                                    databaseId = NULL) {

  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!file.exists(cohortStatisticsFolder)) {
    dir.create(cohortStatisticsFolder, recursive = TRUE)
  }

  # Export the stats
  exportStats <- function(table,
                          fileName) {

    data <- getStatsTable(connection = connection,
                          table = table,
                          snakeCaseToCamelCase = FALSE,
                          databaseId = databaseId,
                          cohortDatabaseSchema = cohortDatabaseSchema)

    fullFileName <- file.path(cohortStatisticsFolder, fileName)
    ParallelLogger::logInfo("- Saving data to - ", fullFileName)
    if (incremental) {
      cohortIds <- unique(data$cohort_definition_id)
      saveIncremental(data, fullFileName, cohort_definition_id = cohortIds)
    } else {
      writeCsv(x = data, file = fullFileName)
    }
  }

  exportStats(cohortTableNames$cohortInclusionTable, "cohort_inclusion.csv")
  exportStats(cohortTableNames$cohortInclusionResultTable, "cohort_inc_result.csv")
  exportStats(cohortTableNames$cohortInclusionStatsTable, "cohort_inc_stats.csv")
  exportStats(cohortTableNames$cohortSummaryStatsTable, "cohort_summary_stats.csv")
  exportStats(cohortTableNames$cohortCensorStatsTable, "cohort_censor_stats.csv")

}
