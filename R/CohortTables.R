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

#' Used to get a list of cohort table names to use when creating the cohort
#' tables
#'
#' @description
#' This function creates a list of table names used by \code{\link{createCohortTables}} to specify
#' the table names to create. Use this function to specify the names of the main cohort table
#' and cohort statistics tables.
#'
#' @param cohortTable                  Name of the cohort table.
#'
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortCensorStatsTable       Name of the censor stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @returns
#' A list of the table names as specified in the parameters to this function.
#'
#' @export
getCohortTableNames <- function(cohortTable = "cohort",
                                cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"),
                                cohortCensorStatsTable = paste0(cohortTable, "_censor_stats")) {
  return(list(
    cohortTable = cohortTable,
    cohortInclusionTable = cohortInclusionTable,
    cohortInclusionResultTable = cohortInclusionResultTable,
    cohortInclusionStatsTable = cohortInclusionStatsTable,
    cohortSummaryStatsTable = cohortSummaryStatsTable,
    cohortCensorStatsTable = cohortCensorStatsTable
  ))
}

#' Create cohort tables
#'
#' @description
#' This function creates an empty cohort table and empty tables for
#' cohort statistics.
#'
#' @template Connection
#'
#' @template CohortTableNames
#'
#' @param incremental                 When set to TRUE, this function will check to see
#'                                    if the cohortTableNames exists in the cohortDatabaseSchema
#'                                    and if they exist, it will skip creating the tables.
#'
#' @export
createCohortTables <- function(connectionDetails = NULL,
                               connection = NULL,
                               cohortDatabaseSchema,
                               cohortTableNames = getCohortTableNames(),
                               incremental = FALSE) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Create a list of the tables to create and determine which require creation
  # if we are incremental mode
  createTableFlagList <- lapply(cohortTableNames, FUN = function(x) {
    x <- TRUE
  })
  if (incremental) {
    tables <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
    for (i in 1:length(cohortTableNames)) {
      if (toupper(cohortTableNames[i]) %in% toupper(tables)) {
        createTableFlagList[i] <- FALSE
        ParallelLogger::logInfo("Table \"", cohortTableNames[i], "\" already exists and in incremental mode, so not recreating it.")
      }
    }
  }

  if (any(unlist(createTableFlagList, use.names = FALSE))) {
    ParallelLogger::logInfo("Creating cohort tables")
    sql <- SqlRender::readSql(system.file("sql/sql_server/CreateCohortTables.sql", package = "CohortGenerator", mustWork = TRUE))
    sql <- SqlRender::render(
      sql = sql,
      cohort_database_schema = cohortDatabaseSchema,
      create_cohort_table = createTableFlagList$cohortTable,
      create_cohort_inclusion_table = createTableFlagList$cohortInclusionTable,
      create_cohort_inclusion_result_table = createTableFlagList$cohortInclusionResultTable,
      create_cohort_inclusion_stats_table = createTableFlagList$cohortInclusionStatsTable,
      create_cohort_summary_stats_table = createTableFlagList$cohortSummaryStatsTable,
      create_cohort_censor_stats_table = createTableFlagList$cohortCensorStatsTable,
      cohort_table = cohortTableNames$cohortTable,
      cohort_inclusion_table = cohortTableNames$cohortInclusionTable,
      cohort_inclusion_result_table = cohortTableNames$cohortInclusionResultTable,
      cohort_inclusion_stats_table = cohortTableNames$cohortInclusionStatsTable,
      cohort_summary_stats_table = cohortTableNames$cohortSummaryStatsTable,
      cohort_censor_stats_table = cohortTableNames$cohortCensorStatsTable,
      warnOnMissingParameters = TRUE
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms
    )
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

    logCreateTableMessage <- function(schema, tableName) {
      ParallelLogger::logInfo("- Created table ", schema, ".", tableName)
    }
    for (i in 1:length(createTableFlagList)) {
      if (createTableFlagList[[i]]) {
        logCreateTableMessage(schema = cohortDatabaseSchema, tableName = cohortTableNames[i])
      }
    }

    delta <- Sys.time() - start
    ParallelLogger::logInfo("Creating cohort tables took ", round(delta, 2), attr(delta, "units"))
  }
}

#' Drop cohort statistics tables
#'
#' @description
#' This function drops the cohort statistics tables.
#'
#' @template Connection
#'
#' @template CohortTableNames
#' @param dropCohortTable       Optionally drop cohort table in addition to stats tables (defaults to FALSE)
#' @export
dropCohortStatsTables <- function(connectionDetails = NULL,
                                  connection = NULL,
                                  cohortDatabaseSchema,
                                  cohortTableNames = getCohortTableNames(),
                                  dropCohortTable = FALSE) {
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Export the stats
  dropTable <- function(table) {
    ParallelLogger::logInfo("- Dropping ", table)
    sql <- "TRUNCATE TABLE @cohort_database_schema.@table;
            DROP TABLE @cohort_database_schema.@table;"
    DatabaseConnector::renderTranslateExecuteSql(
      sql = sql,
      connection = connection,
      table = table,
      cohort_database_schema = cohortDatabaseSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  dropTable(cohortTableNames$cohortInclusionTable)
  dropTable(cohortTableNames$cohortInclusionResultTable)
  dropTable(cohortTableNames$cohortInclusionStatsTable)
  dropTable(cohortTableNames$cohortSummaryStatsTable)
  dropTable(cohortTableNames$cohortCensorStatsTable)

  if (dropCohortTable) {
    dropTable(cohortTableNames$cohortTable)
  }
}
