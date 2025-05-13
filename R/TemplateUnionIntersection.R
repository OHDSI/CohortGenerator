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

#' Create cohort template to intersect multiple cohorts
#'
#' @param cohortDatabaseSchema Schema where cohort tables are stored.
#' @param cohortTable Name of the cohort table.
#' @param cohortIds A vector of `cohort_definition_id` values for the input cohorts.
#' @param intersectCohortId The `cohort_definition_id` for the resulting intersection cohort.
#' @export
createIntersectionCohortTemplate <- function(cohortDatabaseSchema,
                                             cohortTable,
                                             cohortIds,
                                             intersectCohortId) {
  # Validate inputs
  checkmate::assertString(cohortDatabaseSchema)
  checkmate::assertString(cohortTable)
  checkmate::assertNumeric(cohortIds, min.len = 2)  # Require at least two input cohorts to intersect
  checkmate::assertNumeric(intersectCohortId, len = 1)

  # Combine cohort IDs to form a checksum
  combinedChecksum <- digest::digest(sort(cohortIds))  # Sort cohort IDs to ensure consistent checksums

  # SQL template for intersecting multiple cohorts
  intersectSqlTemplate <- "
    INSERT INTO @cohort_database_schema.@cohort_table
      (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    SELECT
      @intersect_cohort_id AS cohort_definition_id,
      subject_id,
      MIN(cohort_start_date) AS cohort_start_date,  -- Choose minimum for overlapping intervals
      MAX(cohort_end_date) AS cohort_end_date      -- Choose maximum for overlapping intervals
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id IN (@cohort_ids)
    GROUP BY subject_id
    HAVING COUNT(DISTINCT cohort_definition_id) = @num_input_cohorts;  -- Ensure subject appears in all cohorts
  "

  # Create references for the resulting intersection cohort
  references <- data.frame(
    cohortId = intersectCohortId,
    cohortName = paste0("Intersection of Cohorts: ", paste(cohortIds, collapse = ", ")),
    sql = intersectSqlTemplate
  )

  # Create cohort template using the CohortTemplateDefinition class
  cohortTemplate <- createCohortTemplateDefintion(
    name = paste0("Intersection Cohort Template: ", combinedChecksum),
    templateSql = intersectSqlTemplate,
    references = references,
    sqlArgs = list(
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      intersect_cohort_id = intersectCohortId,
      cohort_ids = paste(cohortIds, collapse = ", "),  # Pass cohort IDs as a comma-separated string
      num_input_cohorts = length(cohortIds)  # Pass the count of cohorts for the HAVING clause
    ),
    translateSql = TRUE
  )

  return(cohortTemplate)
}

#' Create cohort template to union multiple cohorts
#'
#' @param cohortDatabaseSchema Schema where cohort tables are stored.
#' @param cohortTable Name of the cohort table.
#' @param cohortIds A vector of `cohort_definition_id` values for the input cohorts.
#' @param unionCohortId The `cohort_definition_id` for the resulting union cohort.
#' @export
createUnionCohortTemplate <- function(cohortDatabaseSchema,
                                      cohortTable,
                                      cohortIds,
                                      unionCohortId) {
  # Validate inputs
  checkmate::assertString(cohortDatabaseSchema)
  checkmate::assertString(cohortTable)
  checkmate::assertNumeric(cohortIds, min.len = 2)  # Require at least two input cohorts to union
  checkmate::assertNumeric(unionCohortId, len = 1)

  # Combine cohort IDs to form a checksum
  combinedChecksum <- digest::digest(sort(cohortIds))  # Sort cohort IDs to ensure consistent checksums

  # SQL template for unioning multiple cohorts
  unionSqlTemplate <- "
    INSERT INTO @cohort_database_schema.@cohort_table
      (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    SELECT
      @union_cohort_id AS cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date
    FROM @cohort_database_schema.@cohort_table
    WHERE cohort_definition_id IN (@cohort_ids);"

  # Create references for the resulting union cohort
  references <- data.frame(
    cohortId = unionCohortId,
    cohortName = paste0("Union of Cohorts: ", paste(cohortIds, collapse = ", ")),
    sql = unionSqlTemplate
  )

  # Create cohort template using the CohortTemplateDefinition class
  cohortTemplate <- createCohortTemplateDefintion(
    name = paste0("Union Cohort Template: ", combinedChecksum),
    templateSql = unionSqlTemplate,
    references = references,
    sqlArgs = list(
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      union_cohort_id = unionCohortId,
      cohort_ids = paste(cohortIds, collapse = ", ")  # Pass cohort IDs as a comma-separated string
    ),
    translateSql = TRUE
  )

  return(cohortTemplate)
}
