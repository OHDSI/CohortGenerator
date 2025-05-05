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

.rxNormTemplateRefFun <- function(connection,
                                  cohortDatabaseSchema,
                                  vocabularyDatabaseSchema,
                                  tempEmulationSchema,
                                  rxNormTable,
                                  indentifierExpression) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "rx_norm", "references.sql"),
                                           packageName = utils::packageName(),
                                           identifier_expression = indentifierExpression,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           tempEmulationSchema = tempEmulationSchema,
                                           rx_norm_table = rxNormTable,
                                           vocabulary_database_schema = vocabularyDatabaseSchema)
  DatabaseConnector::executeSql(connection, sql)

  sql <- "SELECT cohort_definition_id as cohort_id, cohort_name FROM @cohort_database_schema.@rx_norm_table;"
  references <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                           sql = sql,
                                                           cohort_database_schema = cohortDatabaseSchema,
                                                           snakeCaseToCamelCase = TRUE,
                                                           rx_norm_table = rxNormTable)
  return(references)
}

.createRxNormCohorts <- function(connection,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTableNames,
                                 vocabularyDatabaseSchema,
                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                 rxNormTable,
                                 priorObservationPeriod = 365) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "rx_norm", "definition.sql"),
                                           dbms = DatabaseConnector::dbms(connection),
                                           packageName = utils::packageName(),
                                           rx_norm_table = rxNormTable,
                                           cohort_table = cohortTableNames$cohortTable,
                                           tempEmulationSchema = tempEmulationSchema,
                                           prior_observation_period = priorObservationPeriod,
                                           vocabulary_database_schema = vocabularyDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cdm_database_schema = cdmDatabaseSchema)

  DatabaseConnector::executeSql(connection, sql)
}

#' Create Rx Norm Cohort Template Definition
#' @description
#' Template cohort definition for all RxNorm ingredients
#' This cohort will use the vocaublary tables to automaticall generate a set of cohorts that have the
#' cohortId = conceptId * 1000, note that this can be customised with the "identifierExpression" if you are using this
#' with other cohorts you may wish to change this to allow uniqueness
#' @param indentifierExpression   an expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param rxNormTable Table to save references in
#' @param priorObservationPeriod (optional) required prior observation period for individuals
#' @inheritParams generateCohortSet
#' @returns a CohortTemplateDefinition instance
#' @export
createRxNormCohortTemplateDefinition <- function(indentifierExpression = "concept_id * 1000",
                                                 cdmDatabaseSchema,
                                                 rxNormTable = "cohort_rx_norm_ref",
                                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                 cohortDatabaseSchema,
                                                 priorObservationPeriod = 365,
                                                 vocabularyDatabaseSchema = cdmDatabaseSchema) {

  executeArgs <- list(
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    priorObservationPeriod = priorObservationPeriod,
    rxNormTable = rxNormTable,
    tempEmulationSchema = tempEmulationSchema
  )

  templateRefArgs <- list(
    cohortDatabaseSchema = cohortDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    indentifierExpression = indentifierExpression,
    rxNormTable = rxNormTable,
    tempEmulationSchema = tempEmulationSchema
  )

  def <- createCohortTemplateDefintion(name = "All ATC 4 class exposures",
                                       templateRefFun = .rxNormTemplateRefFun,
                                       executeFun = .createRxNormCohorts,
                                       templateRefArgs = templateRefArgs,
                                       executeArgs = executeArgs,
                                       requireConnectionRefs = TRUE)

  return(invisible(def))
}

.atcTemplateRefFun <- function(connection,
                               cohortDatabaseSchema,
                               vocabularyDatabaseSchema,
                               tempEmulationSchema,
                               atcTable,
                               indentifierExpression) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "atc", "references.sql"),
                                           packageName = utils::packageName(),
                                           identifier_expression = indentifierExpression,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           tempEmulationSchema = tempEmulationSchema,
                                           atc_table = atcTable,
                                           vocabulary_database_schema = vocabularyDatabaseSchema)
  DatabaseConnector::executeSql(connection, sql)

  sql <- "SELECT cohort_definition_id as cohort_id, cohort_name FROM @cohort_database_schema.@atc_table;"
  references <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                           sql = sql,
                                                           cohort_database_schema = cohortDatabaseSchema,
                                                           snakeCaseToCamelCase = TRUE,
                                                           atc_table = atcTable)
  return(references)
}

.createAtcCohorts <- function(connection,
                              cdmDatabaseSchema,
                              cohortDatabaseSchema,
                              cohortTableNames,
                              vocabularyDatabaseSchema,
                              tempEmulationSchema,
                              atcTable,
                              priorObservationPeriod = 365) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "atc", "definition.sql"),
                                           dbms = DatabaseConnector::dbms(connection),
                                           packageName = utils::packageName(),
                                           atc_table = atcTable,
                                           cohort_table = cohortTableNames$cohortTable,
                                           prior_observation_period = priorObservationPeriod,
                                           vocabulary_database_schema = vocabularyDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cdm_database_schema = cdmDatabaseSchema)

  DatabaseConnector::executeSql(connection, sql)
}

#' Create atc cohort Template Definition
#' @description
#' Template cohort definition for all ATC level 4 class exposures
#' This cohort will use the vocaublary tables to automaticall generate a set of cohorts that have the
#' cohortId = conceptId * 1000 + 4, note that this can be customised with the "identifierExpression" if you are using this
#' with other cohorts you may wish to change this to allow uniqueness
#' @param indentifierExpression   an expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param atcTable  Table to save references in
#' @param priorObservationPeriod (optional) required prior observation period for individuals
#' @inheritParams generateCohortSet
#' @returns a CohortTemplateDefinition instance
#' @export
createAtcCohortTemplateDefinition <- function(indentifierExpression = "concept_id * 1000 + 4",
                                              cdmDatabaseSchema,
                                              atcTable = "cohort_atc_ref",
                                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                              cohortDatabaseSchema,
                                              priorObservationPeriod = 365,
                                              vocabularyDatabaseSchema = cdmDatabaseSchema) {

  executeArgs <- list(
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    priorObservationPeriod = priorObservationPeriod,
    atcTable = atcTable,
    tempEmulationSchema = tempEmulationSchema
  )

  templateRefArgs <- list(
    cohortDatabaseSchema = cohortDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    indentifierExpression = indentifierExpression,
    atcTable = atcTable,
    tempEmulationSchema = tempEmulationSchema
  )

  def <- createCohortTemplateDefintion(name = "All rxNorm Ingredients",
                                       templateRefFun = .atcTemplateRefFun,
                                       executeFun = .createAtcCohorts,
                                       templateRefArgs = templateRefArgs,
                                       executeArgs = executeArgs,
                                       requireConnectionRefs = TRUE)

  return(invisible(def))
}


.snomedTemplateRefFun <- function(connection,
                                  cohortDatabaseSchema,
                                  vocabularyDatabaseSchema,
                                  tempEmulationSchema,
                                  conditionsTable,
                                  includeDescendants,
                                  indentifierExpression) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "snomed", "references.sql"),
                                           packageName = utils::packageName(),
                                           identifier_expression = indentifierExpression,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           tempEmulationSchema = tempEmulationSchema,
                                           conditions_table = conditionsTable,
                                           vocabulary_database_schema = vocabularyDatabaseSchema)
  DatabaseConnector::executeSql(connection, sql)

  sql <- "SELECT cohort_definition_id as cohort_id, cohort_name FROM @cohort_database_schema.@atc_table;"
  references <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                           sql = sql,
                                                           cohort_database_schema = cohortDatabaseSchema,
                                                           snakeCaseToCamelCase = TRUE,
                                                           atc_table = atcTable)
  return(references)
}

.createSnomeCohorts <- function(connection,
                                cdmDatabaseSchema,
                                cohortDatabaseSchema,
                                cohortTableNames,
                                vocabularyDatabaseSchema,
                                tempEmulationSchema,
                                conditionsTable,
                                priorObservationPeriod = 365) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("templates", "snomed", "definition.sql"),
                                           dbms = DatabaseConnector::dbms(connection),
                                           packageName = utils::packageName(),
                                           conditions_table = conditionsTable,
                                           cohort_table = cohortTableNames$cohortTable,
                                           prior_observation_period = priorObservationPeriod,
                                           vocabulary_database_schema = vocabularyDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cdm_database_schema = cdmDatabaseSchema)

  DatabaseConnector::executeSql(connection, sql)
}

#' Create SNOMED cohort Template Definition
#' @description
#' Template cohort definition for all OHDSI standard conditions
#' This cohort will use the vocaublary tables to automaticall generate a set of cohorts that have the
#' cohortId = conceptId * 1000 + 4, note that this can be customised with the "identifierExpression" if you are using this
#' with other cohorts you may wish to change this to allow uniqueness
#' @param indentifierExpression   an expression for setting the cohort id for the resulting cohort. Must produce unique ids
#' @param conditionsTable reference table to store condition cohorts
#' @param priorObservationPeriod (optional) required prior observation period for individuals
#' @inheritParams generateCohortSet
#' @returns a CohortTemplateDefinition instance
#' @export
createSnomedCohortTemplateDefinition <- function(indentifierExpression = "concept_id * 1000",
                                                 cdmDatabaseSchema,
                                                 conditionsTable = "cohort_conditions_ref",
                                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                 cohortDatabaseSchema,
                                                 priorObservationPeriod = 365,
                                                 vocabularyDatabaseSchema = cdmDatabaseSchema) {

  executeArgs <- list(
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    priorObservationPeriod = priorObservationPeriod,
    conditionsTable = conditionsTable,
    tempEmulationSchema = tempEmulationSchema,
    includeDescendants = includeDescendants
  )

  templateRefArgs <- list(
    cohortDatabaseSchema = cohortDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    indentifierExpression = indentifierExpression,
    conditionsTable = conditionsTable,
    tempEmulationSchema = tempEmulationSchema,
    includeDescendants = includeDescendants
  )

  def <- createCohortTemplateDefintion(name = "All SNOMED Conditions",
                                       templateRefFun = .snomedTemplateRefFun,
                                       executeFun = .createSnomeCohorts,
                                       templateRefArgs = templateRefArgs,
                                       executeArgs = executeArgs,
                                       requireConnectionRefs = TRUE)
}