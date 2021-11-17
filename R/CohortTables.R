# Copyright 2021 Observational Health Data Sciences and Informatics
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
#' A list of table names as specified in the parameters of this function.
#' 
#' @export
getCohortTableNames <- function(cohortTable = "cohort",
                                cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"),
                                cohortCensorStatsTable = paste0(cohortTable, "_censor_stats")) {
  return(list(cohortTable = cohortTable,
              cohortInclusionTable = cohortInclusionTable,
              cohortInclusionResultTable = cohortInclusionResultTable,
              cohortInclusionStatsTable = cohortInclusionStatsTable,
              cohortSummaryStatsTable = cohortSummaryStatsTable,
              cohortCensorStatsTable = cohortCensorStatsTable))
}

#' Create cohort tables
#'
#' @description
#' This function creates an empty cohort table and empty tables for
#' cohort statistics. NOTE: This function will drop the tables if they 
#' already exist in your cohortDatabaseSchema. You can use 
#' \code{\link[DatabaseConnector::getTableNames]{DatabaseConnector::getTableNames}}
#' to check if these tables exist before calling this function.
#'
#' @template Connection
#'
#' @param cohortDatabaseSchema        The schema to hold the cohort tables. Note that for
#'                                    SQL Server, this should include both the database and schema
#'                                    name, for example 'scratch.dbo'.
#'
#' @param cohortTableNames            The names of the cohort tables. See \code{\link{getCohortTableNames}}
#'                                    for more details.
#'
#' @export
createCohortTables <- function(connectionDetails = NULL,
                               connection = NULL,
                               cohortDatabaseSchema,
                               cohortTableNames = getCohortTableNames()) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort tables")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::readSql(system.file("sql/sql_server/CreateCohortTables.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::render(sql = sql, 
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTableNames$cohortTable,
                           cohort_inclusion_table = cohortTableNames$cohortInclusionTable,
                           cohort_inclusion_result_table = cohortTableNames$cohortInclusionResultTable,
                           cohort_inclusion_stats_table = cohortTableNames$cohortInclusionStatsTable,
                           cohort_summary_stats_table = cohortTableNames$cohortSummaryStatsTable,
                           cohort_censor_stats_table = cohortTableNames$cohortCensorStatsTable,                           
                           warnOnMissingParameters = TRUE)
  sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  logCreateTableMessage <- function(schema, tableName) {
    ParallelLogger::logInfo("- Created table ", schema, ".", tableName)
  }
  mapply(logCreateTableMessage, schema=cohortDatabaseSchema, tableName=cohortTableNames)

  delta <- Sys.time() - start
  ParallelLogger::logInfo("Creating cohort tables took ", round(delta, 2), attr(delta, "units"))
}