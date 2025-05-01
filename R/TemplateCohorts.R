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
#' This approach is also useful for cohorts that are not based on ATLAS/CirceDefinitions
#'
#'
#' @section Public Functions:
#' \describe{
#'   \item{\code{initialize}}{Initializes the CohortTemplate object with the specified
#'     \code{templateRefFun} and \code{executeFun} functions, along with their
#'     respective arguments.}
#'   \item{\code{executeTemplateSql}}{Executes the SQL queries defined in the
#'     \code{executeFun} function.}
#'   \item{\code{getTemplateReferences}}{Executes the \code{templateRefFun} function
#'     and retrieves template references, ensuring they are returned as data frames.}
#' }
#'
CohortTemplateDefinition <- R6::R6Class(
  "CohortTemplateDefinition",
  private = list(
    .id = NULL,
    .checksum = NULL,
    .name = NULL,
    generateId = function(...) {
      private$.checksum <- digest::digest(list(...))
      private$.id <- paste0("CohortTemplate_", private$.checksum)
    }
  ),
  public = list(
    executeArgs = NULL,
    templateRefArgs = NULL,
    requireConnectionRefs = NULL,
    templateRefFun = NULL,
    executeFun = NULL,
    initialize = function(name, templateRefFun, executeFun, templateRefArgs = list(), executeArgs = list(), requireConnectionRefs = FALSE) {
      # Check if templateRefFun and executeFun are functions
      checkmate::assertFunction(templateRefFun)
      checkmate::assertFunction(executeFun)
      checkmate::assertList(templateRefArgs)
      checkmate::assertList(executeArgs)

      checkmate::assertTRUE("connection" %in% names(formals(executeFun)))

      if (requireConnectionRefs) {
        checkmate::assertTRUE("connection" %in% names(formals(templateRefFun)))
      }

      private$.name <- name
      private$generateId(templateRefFun, executeFun, templateRefArgs, executeArgs)
      self$templateRefFun <- templateRefFun
      self$executeFun <- executeFun
      self$templateRefArgs <- templateRefArgs
      self$executeArgs <- executeArgs
      self$requireConnectionRefs <- requireConnectionRefs
    },

    executeTemplateSql = function(connection,
                                  cohortDatabaseSchema,
                                  cdmDatabaseSchema,
                                  tempEmulationSchema,
                                  cohortTableNames) {

      args <- self$executeArgs
      args$connection <- connection
      args$cohortDatabaseSchema <- cohortDatabaseSchema
      args$cdmDatabaseSchema <- cdmDatabaseSchema
      args$cohortTableNames <- cohortTableNames
      args$tempEmulationSchema <- tempEmulationSchema
      do.call(self$executeFun, args)
    },

    getTemplateReferences = function(connection = NULL) {
      args <- self$templateRefArgs
      # Call templateRefFun and check if it returns a data frame
      if (self$requireConnectionRefs) {
        args$connection <- connection
      }

      result <- do.call(self$templateRefFun, args)
      checkmate::assertDataFrame(result)
      return(result)
    },

    getName = function() {
      return(private$.name)
    },

    getId = function() {
      return(private$.id)
    },

    getChecksum = function() {
      return(private$.checksum)
    }
  )
)

#' Create Cohort Template Definition
#' @description construct a cohort template definition
createCohortTemplateDefintion <- function(name,
                                          templateRefFun,
                                          executeFun,
                                          templateRefArgs,
                                          executeArgs,
                                          requireConnectionRefs) {
  # templateRefFun, executeFun, templateRefArgs = list(), executeArgs = list(), requireConnectionRefs = FALSE
  def <- CohortTemplateDefinition$new(name = name,
                                      templateRefFun = templateRefFun,
                                      executeFun = executeFun,
                                      templateRefArgs = templateRefArgs,
                                      executeArgs = executeArgs,
                                      requireConnectionRefs = requireConnectionRefs)

  return(invisible(def))
}

.getTemplateDefinitions <- function(cohortDefinitionSet) {
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
#' @param connection                An optional connection. If the cohort
#' @param cohortTemplateDefintion   An instance of CohortTemplateDefinition (or subclass)
addCohortTemplateDefintion <- function(cohortDefinitionSet = createEmptyCohortDefinitionSet(),
                                       connection = NULL,
                                       cohortTemplateDefintion) {
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertR6(cohortTemplateDefintion, "CohortTemplateDefinition")

  if (is.null(connection) & cohortTemplateDefintion$requireConnectionRefs)
    stop("Template definition requires connection to CDM to generate references (e.g. for use of vocabulary tables)")

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

  references <- cohortTemplateDefintion$getTemplateReferences(connection = connection)
  if (nrow(references) == 0) {
    stop("No references found")
  }

  checkmate::assertNames(colnames(references),
                         must.include = c(
                           "cohortId",
                           "cohortName"
                         )
  )

  if (!"json" %in% colnames(references)) {
    references$json <- paste("{}")
  }

  # Cohort ID in sql for unqiueness in checksum
  if (is.null(references$sql))
    references$sql <- paste0("SELECT '", references$cohortId, " - ", cohortTemplateDefintion$getName(), "';")

  references$isTemplatedCohort <- TRUE

  # Assert ids are not in
  if (any(references$cohortId %in% cohortDefinitionSet$cohortId)) {
    stop("Cannot add reference set to cohort as it would result in non-unique cohort identifiers")
  }

  templateDefs[[tplId]] <- cohortTemplateDefintion
  attr(cohortDefinitionSet, "templateCohortDefinitions") <- templateDefs
  cohortDefinitionSet <- dplyr::bind_rows(cohortDefinitionSet,
                                          references)

  return(cohortDefinitionSet)
}
