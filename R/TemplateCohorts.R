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

#' Class for automating the creation of bulk cohorts
#'
#' This class provides a framework for automating the creation of bulk cohorts
#' by defining template SQL queries and associated callbacks to execute them.
#' This is useful when defining lots of exposure or outcomes for cohorts that are very general in nature.
#' For example, all rxNorm ingredient cohorts, all ATC ingredient cohorts or all SNOMED condition occurences with > x
#' diagnosis codes.
#'
#' These cohorts can then be subsetted with common cohort subset operations such as limiting to specific age, gender,
#' or observation criteria, should this be excluded from the cohort definition. However, when applying operations in
#' bulk it may be more efficient to include such definitions within the template sql itself.
#'
#' This approach is also useful for cohorts that can not based on ATLAS/CirceDefinitions alone.
#'
#' CURRENTLY NOT SUPPORTED:
#'  * Saving definitions that use runtime arguments on a per cdm basis. This creates a challenge for running the same
#'    cohort across different databases. Furthermore, saving information within the CDM schema in a shared OHDSI study
#'    is not desirable.
CohortTemplateDefinition <- R6::R6Class(
  "CohortTemplateDefinition",
  private = list(
    .id = NULL,
    .checksum = NULL,
    .name = NULL,
    .references = NULL,
    generateId = function(...) {
      private$.checksum <- digest::digest(list(...))
      private$.id <- paste0("CohortTemplate_", private$.checksum)
    }
  ),
  public = list(
    sqlArgs = NULL,
    templateSql = NULL,
    translateSql = FALSE,
        #' @param name                    A name for the template definition. This is not used in the checksum of the cohort
        #'
        #' @param sqlArgs                 Optional parameters for execution of the query - for example vocabulary schema
        #'                                These are arguments that should be passed to the sql. These are used in the checksum
        #'                                if using paramtaried sql for different definitions (e.g. a definition requiring
        #'                                varying observation lengths. This is used to distingish them)
        #'                                This should not include cdm/data source
        #'                                specfic parameters such as the cohort table names,
        #'                                cdm database schema or vocabulary database schema. If the definition requires
        #'                                runtime specific arguments (e.g. non standard tables) this presents a problem
        #'                                for seralizing and uniqiuely idenitifying template cohort definitions.
        #' @param references              This is a data frame that must contain cohortId and cohortName. Optionally, this
        #'                                can contain the columns sql and json as well. It must be bindable to a
        #'                                cohort definition set instance.
        #' @param templateSql             Sql string that is used to generate the cohorts. This should be in OHDSI sql
        #'                                form, translatable to other db platforms.
        #' @param translateSql            to translate the sql or not.
    initialize = function(name,
                          references,
                          templateSql,
                          sqlArgs = list(),
                          translateSql = TRUE) {
      # Check if eecuteFun are functions
      checkmate::assertList(sqlArgs)

      warnFields <- c("cohort_database_schema", "cdm_database_schema", "vocabulary_database_schema", "cohort_table")
      if (any(warnFields %in% names(sqlArgs)))
        warning(paste("Fields", paste(warnFields, collapse = ", "), "should not be included in template definitions"))

      checkmate::assertString(templateSql)
      checkmate::assertString(name)
      # Must have cohort references
      checkmate::assertDataFrame(references, min.rows = 1)
      checkmate::assertNames(colnames(references), must.include = c("cohortId", "cohortName"))
      checkmate::assertLogical(translateSql)
      self$translateSql <- translateSql
      self$templateSql <- templateSql
      private$.name <- name
      private$generateId(references, templateSql, sqlArgs)
      self$sqlArgs <- sqlArgs

      if (!"json" %in% colnames(references)) {
        references$json <- paste("{}")
      }

      # Cohort ID in sql for unqiueness in checksum
      if (is.null(references$sql))
        references$sql <- paste0("SELECT '", references$cohortId, " - ", self$getName(), "';")

      references$isTemplatedCohort <- TRUE
      private$.references <- references
    },

        #' To alter the execution, override this function in a subclass.
        #' This translates and executes the sql
        #' Note that calling this function will not update the checksums in the database so it is unlikely that you will
        #' ever want to call this function outside of a testing environment.
        #' It is best practice to always use the standard runCohortGeneration/generateCohortSet pipeline to ensure
        #' validity of execution steps.
        #' @inheritParams generateCohortSet
    executeTemplateSql = function(connection,
                                  cohortDatabaseSchema,
                                  cdmDatabaseSchema,
                                  cohortTableNames,
                                  vocabularyDatabaseSchema = cdmDatabaseSchema,
                                  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
      checkmate::assertTRUE(DatabaseConnector::dbIsValid(connection))
      checkmate::assertString(cohortDatabaseSchema)
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(tempEmulationSchema, null.ok = TRUE)
      checkmate::assertList(cohortTableNames)
      checkmate::assertNames(names(cohortTableNames), must.include = "cohortTable")

      args <- self$sqlArgs
      args$sql <- self$templateSql
      args$cohort_database_schema <- cohortDatabaseSchema
      args$cdm_database_schema <- cdmDatabaseSchema
      args$cohort_table <- cohortTableNames$cohortTable
      sql <- do.call(SqlRender::render, args)
      if (self$translateSql) {
        args$tempEmulationSchema <- tempEmulationSchema
        sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection))
      }

      DatabaseConnector::executeSql(connection, sql)
    },
        #' get template references data.frame
    getTemplateReferences = function() {
      return(private$.references)
    },

        #' get the name of the definition
    getName = function() {
      return(private$.name)
    },

        #' get the generated id of the template definition
        #' @description
        #' this is not the cohort ids and is based off of the checksum of the template definition
    getId = function() {
      return(private$.id)
    },

        #' get checksum
        #' @description
        #' Get the hash of the definition (generated when class is instantiated)
    getChecksum = function() {
      return(private$.checksum)
    },

        #' to list
        #' @description
        #' For seralizing the definition
    toList = function() {
      def <- list(
        name = self$getName(),
        references = self$getTemplateReferences(),
        templateSql = self$templateSql,
        sqlArgs = self$sqlArgs,
        translateSql = self$translateSql
      )
      return(def)
    },

        #' to json
        #' @description
        #' json seraalized form of the template definition
    toJson = function() {
      .toJSON(self$toList())
    }
  )
)

#' Create Cohort Template Definition
#' @description construct a cohort template definition
createCohortTemplateDefintion <- function(name,
                                          templateSql,
                                          references,
                                          sqlArgs = list(),
                                          translateSql = TRUE) {
  def <- CohortTemplateDefinition$new(name = name,
                                      sqlArgs = sqlArgs,
                                      references = references,
                                      templateSql = templateSql,
                                      translateSql = translateSql)
  return(invisible(def))
}

#' Extract template definitions from a cohort definition set
#' @template cohortDefinitionSet
getTemplateDefinitions <- function(cohortDefinitionSet) {
  checkmate::assertDataFrame(cohortDefinitionSet, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )

  templates <- attr(cohortDefinitionSet, "templateCohortDefinitions")
  if (is.null(templates)) {
    templates <- list()
  }
  return(templates)
}


#' Add Cohort template definition to cohort set
#' @description Adds a cohort template definition to an existing cohort definition set or creates one if none provided
#' @inheritParams generateCohortSet
#' @export
#' @param references                A data.frame containing all cohort ids and names for cohorts. Cohort Id must be
#'                                  unique and not clash with any cohorts in the existing cohort definition set.
#'                                  Json can optionally be provided. For example, in the case where the sql is intended
#'                                  as a bulk speed up, it is likely possible to have an equivalent Circe cohort
#'                                  definition.
#' @param cohortTemplateDefintion   An instance of CohortTemplateDefinition (or subclass)
addCohortTemplateDefintion <- function(cohortDefinitionSet = createEmptyCohortDefinitionSet(),
                                       cohortTemplateDefintion) {
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertR6(cohortTemplateDefintion, "CohortTemplateDefinition")

  if (is.null(attr(cohortDefinitionSet, "templateCohortDefinitions"))) {
    attr(cohortDefinitionSet, "templateCohortDefinitions") <- list()

    if (nrow(cohortDefinitionSet) > 0)
      cohortDefinitionSet$isTemplatedCohort <- FALSE
  }
  tplId <- cohortTemplateDefintion$getId()
  templateDefs <- attr(cohortDefinitionSet, "templateCohortDefinitions")
  if (tplId %in% names(templateDefs)) {
    stop("Template definition with the same ID already added to cohort definition set")
  }

  references <- cohortTemplateDefintion$getTemplateReferences()
  # Assert ids are not in
  if (any(references$cohortId %in% cohortDefinitionSet$cohortId)) {
    stop("Cannot add reference set to cohort as it would result in non-unique cohort identifiers")
  }

  templateDefs[[tplId]] <- cohortTemplateDefintion
  attr(cohortDefinitionSet, "templateCohortDefinitions") <- templateDefs
  cohortDefinitionSet <- dplyr::bind_rows(cohortDefinitionSet, references)

  return(cohortDefinitionSet)
}
