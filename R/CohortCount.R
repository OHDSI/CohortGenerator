# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' Count the cohort(s)
#'
#' @description
#' Computes the subject and entry count per cohort
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds            The cohort Id(s) used to reference the cohort in the cohort
#'                             table. If left empty, all cohorts in the table will be included.
#'
#' @return
#' A data frame with cohort counts
#'
#' @export
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortCounts.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::render(sql = sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable,
                           cohort_ids = cohortIds)
  sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
  tablesInServer <- tolower(DatabaseConnector::getTableNames(conn = connection, databaseSchema = cohortDatabaseSchema))
  if (tolower(cohortTable) %in% tablesInServer) {
    counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Counting cohorts took", signif(delta, 3), attr(delta, "units")))
    return(counts)
  } else {
    warning('Cohort table was not found. Was it created?')
    return(NULL)
  }
}
