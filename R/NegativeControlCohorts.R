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
  df <- data.frame(
    cohortId = numeric(),
    cohortName = character(),
    outcomeConceptId = numeric()
  )
  if (verbose) {
    print(df)
  }
  invisible(df)
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
#' @template CohortTableNames

#' @template TempEmulationSchema
#'
#' @template CohortDatabaseSchema
#'
#' @template CohortTableNames
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
#' @param incremental             Create only cohorts that haven't been created before?
#'
#' @param incrementalFolder       If \code{incremental = TRUE}, specify a folder where records are
#'                                kept of which definition has been executed.
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
                                                  cohortTableNames = getCohortTableNames(),
                                                  cohortTable = cohortTableNames$cohortTable,
                                                  negativeControlOutcomeCohortSet,
                                                  occurrenceType = "all",
                                                  incremental = FALSE,
                                                  incrementalFolder = NULL,
                                                  detectOnDescendants = FALSE) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  checkmate::assert_character(cdmDatabaseSchema, min.chars = 1)
  checkmate::assert_choice(x = tolower(occurrenceType), choices = c("all", "first"))
  checkmate::assert_logical(detectOnDescendants)
  checkmate::assertNames(colnames(negativeControlOutcomeCohortSet),
                         must.include = names(createEmptyNegativeControlOutcomeCohortSet())
  )
  checkmate::assert_data_frame(
    x = negativeControlOutcomeCohortSet,
    min.rows = 1
  )
  assertLargeInteger(negativeControlOutcomeCohortSet$cohortId)
  assertLargeInteger(negativeControlOutcomeCohortSet$outcomeConceptId, columnName = "outcomeConceptId")

  # Verify that cohort IDs are not repeated in the negative control
  # cohort definition set before generating
  if (length(unique(negativeControlOutcomeCohortSet$cohortId)) != length(negativeControlOutcomeCohortSet$cohortId)) {
    duplicatedCohortIds <- negativeControlOutcomeCohortSet$cohortId[duplicated(negativeControlOutcomeCohortSet$cohortId)]
    stop("Cannot generate! Duplicate cohort IDs found in your negativeControlOutcomeCohortSet: ", paste(duplicatedCohortIds, sep = ","), ". Please fix your negativeControlOutcomeCohortSet and try again.")
  }

  checksum <- computeChecksum(jsonlite::toJSON(
    list(
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
      occurrenceType = occurrenceType,
      detectOnDescendants = detectOnDescendants
    )
  ))[[1]]

  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }

    recordKeepingFile <- file.path(incrementalFolder, "GeneratedNegativeControls.csv")
    computedChecksums <- getLastGeneratedCohortChecksums(connection = connection,
                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                         cohortTableNames = cohortTableNames)
    if (checksum %in% computedChecksums$checksum) {
      ParallelLogger::logInfo("Negative control set generation skipped")
      return(invisible("SKIPPED"))
    }
  }

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

  rlang::inform("Generating negative control outcome cohorts")

  recordNcCohorts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortChecksumTable = cohortTableNames$cohortChecksumTable,
    negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
    checksum = checksum,
    start = as.numeric(start) * 1000
  )


  sql <- createNegativeControlOutcomesQuery(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    occurrenceType = occurrenceType,
    detectOnDescendants = detectOnDescendants,
    negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )

  recordNcCohorts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortChecksumTable = cohortTableNames$cohortChecksumTable,
    negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
    checksum = checksum,
    start = as.numeric(start) * 1000,
    end = as.numeric(Sys.time()) * 1000
  )


  delta <- Sys.time() - start
  writeLines(paste("Generating negative control outcomes set took", round(delta, 2), attr(delta, "units")))

  if (incremental) {
    recordTasksDone(
      paramHash = checksum,
      checksum = checksum,
      recordKeepingFile = recordKeepingFile
    )
  }

  invisible("FINISHED")
}

createNegativeControlOutcomesQuery <- function(connection,
                                               cdmDatabaseSchema,
                                               tempEmulationSchema,
                                               cohortDatabaseSchema,
                                               cohortTable,
                                               occurrenceType,
                                               detectOnDescendants,
                                               negativeControlOutcomeCohortSet) {
  selectClause <- ""
  for (i in 1:nrow(negativeControlOutcomeCohortSet)) {
    selectClause <- paste0(
      selectClause,
      "SELECT CAST(", negativeControlOutcomeCohortSet$cohortId[i], " AS BIGINT), ",
      "CAST(", negativeControlOutcomeCohortSet$outcomeConceptId[i], " AS BIGINT)"
    )
    if (i < nrow(negativeControlOutcomeCohortSet)) {
      selectClause <- paste0(selectClause, "\nUNION\n")
    }
  }
  selectClause
  ncSetQuery <- paste0(
    "DROP TABLE IF EXISTS #nc_set;",
    "CREATE TABLE #nc_set (",
    "  cohort_id bigint NOT NULL,",
    "  outcome_concept_id bigint NOT NULL",
    ")",
    ";",
    "INSERT INTO #nc_set (cohort_id, outcome_concept_id)\n",
    selectClause,
    "\n;"
  )

  sql <- sql <- SqlRender::readSql(system.file("sql/sql_server/NegativeControlOutcomes.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::render(
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    detect_on_descendants = detectOnDescendants,
    occurrence_type = occurrenceType,
    nc_set_query = ncSetQuery,
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms,
    tempEmulationSchema = tempEmulationSchema
  )
}

# This process could be simplified but the current checksum table uses a cohort id - either a second checksum table or
# allowing this to be a singular id for all cohorts would be a speed up if this becomes a performance issue
recordNcCohorts <- function(connection,
                            cohortDatabaseSchema,
                            cohortChecksumTable,
                            negativeControlOutcomeCohortSet,
                            checksum,
                            start,
                            end = "NULL") {
  # Use delete instead of update to improve performance on MPP platforms
  endSql <- "
  DELETE FROM @results_database_schema.@cohort_checksum_table
  WHERE cohort_definition_id = @target_cohort_id AND checksum = '@checksum';

  INSERT INTO @results_database_schema.@cohort_checksum_table (cohort_definition_id, checksum, start_time, end_time)
  VALUES (@target_cohort_id, '@checksum', @start_time, @end_time);

  "

  sql <- ""
  for (i in 1:nrow(negativeControlOutcomeCohortSet)) {
    sql <- paste(sql, SqlRender::render(endSql,
                                        checksum = checksum,
                                        start_time = start,
                                        target_cohort_id = negativeControlOutcomeCohortSet$cohortId[i],
                                        end_time = end,
                                        results_database_schema = cohortDatabaseSchema,
                                        cohort_checksum_table = cohortChecksumTable,
                                        warnOnMissingParameters = FALSE))
  }


  DatabaseConnector::renderTranslateExecuteSql(connection, sql)
}