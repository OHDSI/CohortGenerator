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

#' Used to insert the inclusion rule names from a cohort definition set
#' when generating cohorts that include cohort statistics
#'
#' @description
#' This function will take a cohortDefinitionSet that inclusions the Circe JSON
#' representation of each cohort, parse the InclusionRule property to obtain
#' the inclusion rule name and sequence number and insert the values into the
#' cohortInclusionTable. This function is only required when generating cohorts
#' that include cohort statistics.
#'
#' @template Connection
#'
#' @template CohortDefinitionSet
#'
#' @template CohortDatabaseSchema
#'
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @returns
#' A data frame containing the inclusion rules by cohort and sequence ID
#'
#' @export
insertInclusionRuleNames <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cohortDefinitionSet,
                                     cohortDatabaseSchema,
                                     cohortInclusionTable = getCohortTableNames()$cohortInclusionTable) {
  # Parameter validation
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
    must.include = c(
      "cohortId",
      "cohortName",
      "json"
    )
  )
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  tableList <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
  if (!toupper(cohortInclusionTable) %in% toupper(tableList)) {
    stop(paste0(cohortInclusionTable, " table not found in schema: ", cohortDatabaseSchema, ". Please make sure the table is created using the createCohortTables() function before calling this function."))
  }

  # Assemble the cohort inclusion rules
  # NOTE: This data frame must match the @cohort_inclusion_table
  # structure as defined in inst/sql/sql_server/CreateCohortTables.sql
  inclusionRules <- data.frame(
    cohortDefinitionId = bit64::integer64(),
    ruleSequence = integer(),
    name = character(),
    description = character()
  )
  # Remove any cohort definitions that do not include the JSON property
  cohortDefinitionSet <- cohortDefinitionSet[!(is.null(cohortDefinitionSet$json) | is.na(cohortDefinitionSet$json)), ]
  for (i in 1:nrow(cohortDefinitionSet)) {
    cohortDefinition <- RJSONIO::fromJSON(content = cohortDefinitionSet$json[i], digits = 23)
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (j in 1:nrOfRules) {
          ruleName <- cohortDefinition$InclusionRules[[j]]$name
          ruleDescription <- cohortDefinition$InclusionRules[[j]]$description
          if (is.na(ruleName) || ruleName == "") {
            ruleName <- paste0("Unamed rule (Sequence ", j - 1, ")")
          }
          if (is.null(ruleDescription)) {
            ruleDescription <- ""
          }
          inclusionRules <- rbind(
            inclusionRules,
            data.frame(
              cohortDefinitionId = bit64::as.integer64(cohortDefinitionSet$cohortId[i]),
              ruleSequence = as.integer(j - 1),
              name = ruleName,
              description = ruleDescription
            )
          )
        }
      }
    }
  }

  # Remove any existing data to prevent duplication
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "TRUNCATE TABLE @cohort_database_schema.@table;",
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    table = cohortInclusionTable
  )

  # Insert the inclusion rules
  if (nrow(inclusionRules) > 0) {
    ParallelLogger::logInfo("Inserting inclusion rule names")
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortInclusionTable,
      data = inclusionRules,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      camelCaseToSnakeCase = TRUE
    )
  } else {
    warning("No inclusion rules found in the cohortDefinitionSet")
  }

  invisible(inclusionRules)
}

# Get stats data
getStatsTable <- function(connectionDetails,
                          connection = NULL,
                          cohortDatabaseSchema,
                          table,
                          snakeCaseToCamelCase = FALSE,
                          databaseId = NULL,
                          includeDatabaseId = TRUE) {
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Force databaseId to NULL when includeDatabaseId is FALSE
  if (!includeDatabaseId) {
    databaseId <- NULL
  }

  ParallelLogger::logInfo("- Fetching data from ", table)
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} * FROM @cohort_database_schema.@table"
  data <- DatabaseConnector::renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    table = table,
    cohort_database_schema = cohortDatabaseSchema,
    database_id = ifelse(test = is.null(databaseId),
      yes = "",
      no = databaseId
    )
  )

  if (!snakeCaseToCamelCase) {
    colnames(data) <- tolower(colnames(data))
  }

  return(data)
}

#' Get Cohort Inclusion Stats Table Data
#' @description
#' This function returns a data frame of the data in the Cohort Inclusion Tables.
#' Results are organized in to a list with 5 different data frames:
#'  * cohortInclusionTable
#'  * cohortInclusionResultTable
#'  * cohortInclusionStatsTable
#'  * cohortSummaryStatsTable
#'  * cohortCensorStatsTable
#'
#'
#' These can be optionally specified with the `outputTables`.
#' See `exportCohortStatsTables` function for saving data to csv.
#'
#' @md
#' @inheritParams exportCohortStatsTables
#'
#' @param snakeCaseToCamelCase        Convert column names from snake case to camel case.
#' @param outputTables                Character vector. One or more of "cohortInclusionTable", "cohortInclusionResultTable",
#'                                    "cohortInclusionStatsTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable"
#'                                    or "cohortCensorStatsTable". Output is limited to these tables. Cannot export, for,
#'                                    example, the cohort table. Defaults to all stats tables.
#' @export
getCohortStats <- function(connectionDetails,
                           connection = NULL,
                           cohortDatabaseSchema,
                           databaseId = NULL,
                           snakeCaseToCamelCase = TRUE,
                           outputTables = c(
                             "cohortInclusionTable",
                             "cohortInclusionResultTable",
                             "cohortInclusionStatsTable",
                             "cohortInclusionStatsTable",
                             "cohortSummaryStatsTable",
                             "cohortCensorStatsTable"
                           ),
                           cohortTableNames = getCohortTableNames()) {
  # Names of cohort table names must include output tables
  checkmate::assertNames(names(cohortTableNames), must.include = outputTables)
  # ouput tables strictly the set of allowed tables
  checkmate::assertNames(outputTables,
    subset.of = c(
      "cohortInclusionTable",
      "cohortInclusionResultTable",
      "cohortInclusionStatsTable",
      "cohortInclusionStatsTable",
      "cohortSummaryStatsTable",
      "cohortCensorStatsTable"
    )
  )
  results <- list()
  for (table in outputTables) {
    # The cohortInclusionTable does not hold database
    # specific information so the databaseId
    # should NOT be included.
    includeDatabaseId <- ifelse(test = table != "cohortInclusionTable",
      yes = TRUE,
      no = FALSE
    )
    results[[table]] <- getStatsTable(
      connectionDetails = connectionDetails,
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      table = cohortTableNames[[table]],
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      includeDatabaseId = includeDatabaseId
    )
  }
  return(results)
}
