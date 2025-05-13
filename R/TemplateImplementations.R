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

#' Create Rx Norm Cohort Template Definition
#' @description
#' Template cohort definition for all RxNorm ingredients. This cohort will use the
#' vocabulary tables to automatically generate a set of cohorts that have the
#' cohortId = conceptId * 1000. The "identifierExpression" can be customized for uniqueness.
#' @param connection Database connection object
#' @param identifierExpression An expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param cdmDatabaseSchema CDM database schema
#' @param rxNormTable Table to save references in
#' @param tempEmulationSchema Temporary emulation schema
#' @param cohortDatabaseSchema Cohort database schema
#' @param priorObservationPeriod (optional) Required prior observation period for individuals
#' @param requireSecondDiagnosis (optional) Logical - require a second diagnosis code for this definition?
#' @returns A CohortTemplateDefinition instance
#' @export
createRxNormCohortTemplateDefinition <- function(connection,
                                                 identifierExpression = "concept_id * 1000",
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                 cohortDatabaseSchema,
                                                 priorObservationPeriod = 365,
                                                 nameSuffix = '',
                                                 vocabularyDatabaseSchema = cdmDatabaseSchema) {

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "rx_norm", "references.sql"),
    packageName = utils::packageName(),
    identifier_expression = identifierExpression,
    tempEmulationSchema = tempEmulationSchema,
    name_suffix = nameSuffix,
    vocabulary_database_schema = vocabularyDatabaseSchema
  )
  references <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  templateSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "rx_norm", "definition.sql"),
    packageName = utils::packageName(),
    prior_observation_period = priorObservationPeriod,
    temp_emulation_schema = tempEmulationSchema,
    identifier_expression = identifierExpression,
    warnOnMissingParameters = FALSE
  )

  def <- createCohortTemplateDefintion(
    name = "All RxNorm ingredient exposures",
    templateSql = templateSql,
    references = references
  )

  return(invisible(def))
}

#' Create ATC Cohort Template Definition
#' @description
#' Template cohort definition for all ATC level 4 class exposures. The cohortId = conceptId * 1000 + 4.
#' The "identifierExpression" can be customized for uniqueness.
#' @param connection Database connection object
#' @param identifierExpression An expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param cdmDatabaseSchema CDM database schema
#' @param atcTable Table to save references in
#' @param tempEmulationSchema Temporary emulation schema
#' @param cohortDatabaseSchema Cohort database schema
#' @param mergeIngredientEras (optional) Boolean indicating if different ingredients under the same ATC code should be merged
#' @param priorObservationPeriod (optional) Required prior observation period for individuals
#' @param vocabularyDatabaseSchema Vocabulary database schema
#' @returns A CohortTemplateDefinition instance
#' @export
createAtcCohortTemplateDefinition <- function(connection,
                                              identifierExpression = "concept_id * 1000",
                                              cdmDatabaseSchema,
                                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                              cohortDatabaseSchema,
                                              nameSuffix = '',
                                              mergeIngredientEras = TRUE,
                                              priorObservationPeriod = 365,
                                              vocabularyDatabaseSchema = cdmDatabaseSchema) {


  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "atc", "references.sql"),
    packageName = utils::packageName(),
    identifier_expression = identifierExpression,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    name_suffix = nameSuffix
  )

  references <- DatabaseConnector::querySql(connection = connection,
                                            sql = sql,
                                            snakeCaseToCamelCase = TRUE)

  templateSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "atc", "definition.sql"),
    packageName = utils::packageName(),
    prior_observation_period = priorObservationPeriod,
    temp_emulation_schema = tempEmulationSchema,
    identifier_expression = identifierExpression,
    merge_ingredient_eras = mergeIngredientEras,
    warnOnMissingParameters = FALSE
  )

  def <- createCohortTemplateDefintion(
    name = "All ATC class exposures",
    templateSql = templateSql,
    references = references
  )

  return(invisible(def))
}

#' Create SNOMED Cohort Template Definition
#' @description
#' Template cohort definition for all OHDSI standard conditions. The cohortId = conceptId * 1000.
#' The "identifierExpression" can be customized for uniqueness.
#' @param connection Database connection object
#' @param identifierExpression An expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param cdmDatabaseSchema CDM database schema
#' @param conditionsTable Reference table to store condition cohorts
#' @param tempEmulationSchema Temporary emulation schema
#' @param cohortDatabaseSchema Cohort database schema
#' @param priorObservationPeriod (optional) Required prior observation period for individuals
#' @param vocabularyDatabaseSchema Vocabulary database schema
#' @returns A CohortTemplateDefinition instance
#' @export
createSnomedCohortTemplateDefinition <- function(connection,
                                                 identifierExpression = "concept_id * 1000",
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                 cohortDatabaseSchema,
                                                 priorObservationPeriod = 365,
                                                 requireSecondDiagnosis = FALSE,
                                                 nameSuffix = '',
                                                 vocabularyDatabaseSchema = cdmDatabaseSchema) {
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "snomed", "references.sql"),
    packageName = utils::packageName(),
    identifier_expression = identifierExpression,
    tempEmulationSchema = tempEmulationSchema,
    require_second_diagnosis = requireSecondDiagnosis,
    name_suffix = nameSuffix,
    vocabulary_database_schema = vocabularyDatabaseSchema
  )

  references <- DatabaseConnector::querySql(connection = connection,
                                            sql = sql,
                                            snakeCaseToCamelCase = TRUE)
  templateSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = file.path("templates", "snomed", "definition.sql"),
    packageName = utils::packageName(),
    identifier_expression = identifierExpression,
    tempEmulationSchema = tempEmulationSchema,
    require_second_diagnosis = requireSecondDiagnosis,
    warnOnMissingParameters = FALSE
  )

  def <- createCohortTemplateDefintion(
    name = paste("All SNOMED Conditions", nameSuffix),
    templateSql = templateSql,
    references = references
  )

  return(invisible(def))
}