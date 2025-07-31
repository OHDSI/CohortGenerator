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

#' Create cohort template to union multiple cohorts
#' @description
#' This is a union between all cohorts within a specified set of ids.
#' If an individual has multiple overlapping eras, they will be merged into a single time window.
#'
#' For example:
#'
#' ```
#' A:    [--------]
#' B:        [-------]
#' C:            [-------]
#' ```
#' Becomes:
#' ```
#' AUBUC: [--------------]
#' ```
#' It is never allowed to have multiple overlapping eras for the same indiviudal within a cohort
#'
#'
#'
#' @param cohortIds A vector of `cohort_definition_id` values for the input cohorts.
#' @param unionCohortId The `cohort_definition_id` for the resulting union cohort.
#' @param cohortName  The Name of the resulting cohort
#' @export
createUnionCohortTemplate <- function(cohortIds,
                                      cohortName,
                                      unionCohortId) {

  checkmate::assertNumeric(cohortIds, min.len = 2)  # Require at least two input cohorts to union
  checkmate::assertNumeric(unionCohortId, len = 1)
  checkmate::assertString(cohortName)

  # Combine cohort IDs to form a checksum
  combinedChecksum <- digest::digest(sort(cohortIds))  # Sort cohort IDs to ensure consistent checksums

  # SQL template for unioning multiple cohorts
  unionSqlTemplate <- "
    INSERT INTO @cohort_database_schema.@cohort_table
      (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    SELECT
      DISTINCT
      @union_cohort_id AS cohort_definition_id,
      subject_id,
      min(cohort_start_date) as cohort_start_date, -- first entry for overlapping era
      max(cohort_end_date) as cohort_end_date -- end entry for overlapping era
    FROM  (
      SELECT
            c.subject_id,
            c.cohort_start_date,
            c.cohort_end_date,
            SUM(
              CASE
                WHEN c.cohort_start_date > LAG(c.cohort_end_date) OVER (
                    PARTITION BY c.subject_id
                    ORDER BY c.cohort_start_date
                ) THEN 1
                ELSE 0
              END
            ) OVER (PARTITION BY c.subject_id ORDER BY c.cohort_start_date) AS group_id
        FROM @cohort_database_scheme.@cohort_table c
        WHERE c.cohort_definition_id IN @cohort_ids
    )
    GROUP BY subject_id group_id
   "

  # Create references for the resulting union cohort
  references <- data.frame(
    cohortId = unionCohortId,
    cohortName = paste0("Union of Cohorts: ", paste(cohortIds, collapse = ", ")),
    sql = unionSqlTemplate
  )

  # Create cohort template using the CohortTemplateDefinition class
  cohortTemplate <- createCohortTemplateDefintion(
    name = cohortName,
    templateSql = unionSqlTemplate,
    references = references,
    sqlArgs = list(
      union_cohort_id = unionCohortId,
      cohort_ids = cohortIds
    ),
    translateSql = TRUE
  )

  return(cohortTemplate)
}

#' Add union cohort definition to cohort definition set
#' @description
#' This utility function adds the union of any two or more cohort ids to the cohort definition set with a new id and name.
#'
#' If a name parameter is not provideded this will be auto generated as the union of the provided cohort id
#' @param cohortDefinitionSet   cohort definition set
#' @inheritParams createUnionCohortTemplate
addUnionCohortDefinition <- function(cohortDefinitionSet,
                                     cohortIds,
                                     cohortName,
                                     unionCohortId) {
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )
  assertLargeInteger(cohortDefinitionSet$cohortId)

  checkmate::assertSubset(cohortIds, cohortDefinitionSet$cohortId)
  checkmate::assertFALSE(unionCohortId %in% cohortDefinitionSet$cohortId)
  tplDef <- createUnionCohortTemplate(cohortIds, cohortName, unionCohortId)
  cohortDefinitionSet <- addCohortTemplateDefintion(cohortDefinitionSet, tplDef)
  return(cohortDefinitionSet)
}