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

#' Validate cohort
#' @description
#' Using custom sql, it is possible to generate cohorts that are not technically definitions.
#' Invalid cohorts include the following:
#'
#' * Cohorts where indiviudals have multiple, overlapping eras
#' * Cohorts that lie outside the observation period for individuals
#' * Cohorts that have start dates that occur after their end dates
#' * Cohorts with duplicate entries for the same subject.
#'
#' Note - this code cannot formally verify the validity of a cohort. There may be situations where the logic of a
#' cohort definition only causes errors in certain circumstances. Furthermore, if cohort counts are 0 this check is
#' unable to evaluate validity at all.
#'
#' The returned data.frame counts the number of errors found for each cohort. In addition a boolean "valid"
#' field is applied that is TRUE only in the case where all counts are 0.
#'
#' @return a data.frame with the fields cohortId, overlappingErasCount, invalidDateCount, duplicateCount, outsideObservationCount
#'
#' @export
#' @inheritParams generateCohortSet
#' @param cohortIds Ids of cohorts to validate
getCohortValidationCounts <- function(connectionDetails = NULL,
                                      connection = NULL,
                                      cdmDatabaseSchema,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      cohortDatabaseSchema = cdmDatabaseSchema,
                                      cohortTableNames = getCohortTableNames(),
                                      cohortIds = NULL) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  start <- Sys.time()
  sql <- SqlRender::loadRenderTranslateSql("ValidateCohorts.sql",
                                           tempEmulationSchema = tempEmulationSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_table = cohortTableNames$cohortTable,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_ids = cohortIds,
                                           packageName = utils::packageName())

  ParallelLogger::logInfo("Computing cohort validation checks")
  result <- DatabaseConnector::renderTranslateQuerySql(connection, sql, snakeCaseToCamelCase = TRUE)
  ParallelLogger::logInfo(paste("Computed validation checks for", nrow(result), "cohorts"))

  result <- result |> dplyr::mutate(
    valid = .data$overlappingErasCount == 0 &
      .data$invalidDateCount == 0 &
      .data$duplicateCount == 0 &
      .data$outsideObservationCount == 0)

  delta <- Sys.time() - start
  writeLines(paste("Generating validation check set took", round(delta, 2), attr(delta, "units")))
  return(result)
}