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
#' @export
#' @inheritParams generateCohortSet
#' @param cohortId Id of cohort to validate
validateCohort <- function(connectionDetails = NULL,
                            connection = NULL,
                            cdmDatabaseSchema,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                            cohortDatabaseSchema = cdmDatabaseSchema,
                            cohortTableNames = getCohortTableNames(),
                            cohortId) {
  sql <- "

  SELECT
    c.cohort_definition_id
    -- NUMBER OF OVERLAPPING ERAS
    sum(c2.person_id) over c2 HAVING not NULL as overlapping_eras,
    -- NUMBER OF INDIVIUDALS WITH COHORTS OUTSIDE OBSERVATION
    sum(op.person_id) as outside_observation,
    -- NUMBER OF INDIVIDUALS THEAT HAVE START DATES THAT OCCUR AFTER END DATES
    -- DUPLICATE ENTRIES FOR A COHORT
   FROM @cohort_database_schema.@cohort_table c

   LEFT JOIN @cdm_database_schema.observaton_period op ON (
      op.person_id = c.subject_id
      AND op.observation_period_start_date > c.cohort_start_date
      OR op.observation_period_end_date < c.cohort_end_date
   )
   LEFT JOIN @cohort_database_schema.@cohort_table c2 ON (
      c.subject_id = c2.subject_id AND c.cohort_definition_id = c2.cohort_definition_id
      AND c2.cohort_start_date > c.cohort_start_date
      AND c2.cohort_end_date <= c.cohort_end_date
    )

    LEFT JOIN @cohort_database_schema.@cohort_table c3 ON (
      c.subject_id = c3.subject_id AND c.cohort_definition_id = c3.cohort_definition_id
      AND c3.cohort_start_date > c3.cohort_end_date
    )
   GROUP BY c.cohort_definition_id

  "
}


#' Validate cohorts within a cohort definition set
#' @description
#' Utility function that allows poping a cohort definition set
validateCohortDefinitionSet <- function(cohortDefinitionSet, cohortIds = NULL) {
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )
  assertLargeInteger(cohortDefinitionSet$cohortId)
  assertLargeInteger(cohortIds, null.ok = TRUE)
  if (is.null(cohortIds))
    cohortIds <- cohortDefinitionSet$cohortId

  if (is.null(cohortDefinitionSet$validated)) {
    cohortDefinitionSet$validated <- FALSE
  }


}