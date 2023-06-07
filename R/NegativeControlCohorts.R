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

#' Create an empty negative control outcome cohort set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{generateNegativeControlOutcomeCohorts}.
#'
#' @param verbose When TRUE, descriptions of each field in the data.frame are
#'                returned
#'
#' @return
#' Invisibly returns an empty negative control outcome cohort set data.frame
#'
#' @export
createEmptyNegativeControlOutcomeCohortSet <- function(verbose = FALSE) {
  checkmate::assert_logical(verbose)
  negativeControlOutcomeCohortSetSpecification <- .getNegativeControlOutcomeCohortSetSpecification()
  if (verbose) {
    print(negativeControlOutcomeCohortSetSpecification)
  }
  # Build the data.frame dynamically
  df <- .createEmptyDataFrameFromSpecification(negativeControlOutcomeCohortSetSpecification)
  invisible(df)
}

#' Helper function to return the specification description of a
#' negativeControlOutcomeCohortSet
#'
#' @description
#' This function reads from the negativeControlOutcomeCohortSetSpecificationDescription.csv
#' to return a data.frame that describes the required columns in a
#' negativeControlOutcomeCohortSet
#'
#' @return
#' Returns a data.frame that defines a negativeControlOutcomeCohortSet
#'
#' @noRd
#' @keywords internal
.getNegativeControlOutcomeCohortSetSpecification <- function() {
  return(readCsv(system.file("negativeControlOutcomeCohortSetSpecificationDescription.csv",
    package = "CohortGenerator",
    mustWork = TRUE
  )))
}


#' Generate a set of negative control outcome cohorts
#'
#' @description
#' This function generate a set of negative control outcome cohorts.
#' For more information please see [Chapter 12 - Population Level Estimation](https://ohdsi.github.io/TheBookOfOhdsi/PopulationLevelEstimation.html)
#' for more information how these cohorts are utilized in a study design.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortDatabaseSchema
#'
#' @param cohortTable                  Name of the cohort table.
#'
#' @template negativeControlOutcomeCohortSet
#'
#' @param occurrenceType     The occurrenceType will detect either: the first time an outcomeConceptId occurs
#'                           or all times the outcomeConceptId occurs for a person. Values accepted: 'all' or 'first'.
#'
#' @param detectOnDescendants     When set to TRUE, detectOnDescendants will use the vocabulary to find negative control
#'                                outcomes using the outcomeConceptId and all descendants via the concept_ancestor table.
#'                                When FALSE, only the exact outcomeConceptId will be used to detect the outcome.
#'
#' @return
#' Invisibly returns an empty negative control outcome cohort set data.frame
#'
#' @export
generateNegativeControlOutcomeCohorts <- function(connectionDetails = NULL,
                                                  connection = NULL,
                                                  cdmDatabaseSchema,
                                                  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                  cohortDatabaseSchema = cdmDatabaseSchema,
                                                  cohortTable = getCohortTableNames()$cohortTable,
                                                  negativeControlOutcomeCohortSet,
                                                  occurrenceType = "all",
                                                  detectOnDescendants = FALSE) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  checkmate::assert_character(cdmDatabaseSchema, min.chars = 1)
  checkmate::assert_choice(x = tolower(occurrenceType), choices = c("all", "first"))
  checkmate::assert_logical(detectOnDescendants)
  checkmate::assertNames(colnames(negativeControlOutcomeCohortSet),
    must.include = .getNegativeControlOutcomeCohortSetSpecification()$columnName
  )
  checkmate::assert_data_frame(
    x = negativeControlOutcomeCohortSet,
    min.rows = 1
  )

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Error if cohortTable does not exist
  tableList <- DatabaseConnector::getTableNames(
    connection = connection,
    databaseSchema = cohortDatabaseSchema
  )
  if (!tolower(cohortTable) %in% tolower(tableList)) {
    stop(paste0("Table: ", cohortTable, " not found in schema: ", cohortDatabaseSchema, ". Please use `createCohortTable` to ensure the cohort table is created before generating cohorts."))
  }

  ParallelLogger::logInfo("Generating negative control outcome cohorts")

  # Send the negative control outcome cohort set to the server for use
  # in processing. This temp table will hold the mapping between
  # cohort_definition_id and the outcomeConceptId in the data.frame()
  DatabaseConnector::insertTable(
    connection = connection,
    data = negativeControlOutcomeCohortSet,
    tempEmulationSchema = tempEmulationSchema,
    tableName = "#nc_set",
    camelCaseToSnakeCase = TRUE,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE
  )

  sql <- createNegativeControlOutcomesQuery(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    occurrenceType = occurrenceType,
    detectOnDescendants = detectOnDescendants
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )
  delta <- Sys.time() - start
  writeLines(paste("Generating negative control outcomes set took", round(delta, 2), attr(delta, "units")))
}

createNegativeControlOutcomesQuery <- function(connection,
                                               cdmDatabaseSchema,
                                               tempEmulationSchema,
                                               cohortDatabaseSchema,
                                               cohortTable,
                                               occurrenceType,
                                               detectOnDescendants) {
  sql <- sql <- SqlRender::readSql(system.file("sql/sql_server/NegativeControlOutcomes.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    detect_on_descendants = detectOnDescendants,
    occurrence_type = occurrenceType,
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms,
    tempEmulationSchema = tempEmulationSchema
  )
}
