# Copyright 2026 Observational Health Data Sciences and Informatics
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

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  tableList <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
  if (!toupper(cohortInclusionTable) %in% toupper(tableList)) {
    stop(paste0(cohortInclusionTable, " table not found in schema: ", cohortDatabaseSchema, ". Please make sure the table is created using the createCohortTables() function before calling this function."))
  }

  inclusionRules <- getCohortInclusionRules(cohortDefinitionSet)

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
    rlang::inform("Inserting inclusion rule names")
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

  rlang::inform(paste0("- Fetching data from ", table))
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} t.* FROM @cohort_database_schema.@table t"
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
#'
#' @description
#' This function returns a data frame of the data in the Cohort Inclusion Tables.
#' Results are organized in to a list with 6 different data frames:
#'  * cohortInclusionTable
#'  * cohortInclusionResultTable
#'  * cohortInclusionStatsTable
#'  * cohortSummaryStatsTable
#'  * cohortCensorStatsTable
#'  * cohortAttritionTable
#'
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
#'                                    or "cohortCensorStatsTable", "cohortAttritionTable". Output is limited to these tables. Cannot export, for,
#'                                    example, the cohort table. Defaults to all stats tables.
#' @param inclusionRules              A data.frame with inclusion rules from the cohortDefinitionSet used to generate
#'                                    the cohort stats obtained by running `getCohortInclusionRules(cohortDefinitionSet)` (Optional)
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
                             "cohortCensorStatsTable",
                             "cohortAttritionTable"
                           ),
                           cohortTableNames = getCohortTableNames(),
                           inclusionRules = NULL) {
  # Names of cohort table names must include output tables
  requiredTables <- setdiff(outputTables, "cohortAttritionTable")
  checkmate::assertNames(names(cohortTableNames), must.include = requiredTables)
  # ouput tables strictly the set of allowed tables
  checkmate::assertNames(outputTables,
    subset.of = c(
      "cohortInclusionTable",
      "cohortInclusionResultTable",
      "cohortInclusionStatsTable",
      "cohortInclusionStatsTable",
      "cohortSummaryStatsTable",
      "cohortCensorStatsTable",
      "cohortAttritionTable"
    )
  )

  # cohortAttritionTable is derived (not a physical DB table). Track the caller's
  # requested tables, fetch the required inputs, then compute attrition in R.
  requestedTables <- outputTables
  if ("cohortAttritionTable" %in% outputTables) {
    outputTables <- setdiff(outputTables, "cohortAttritionTable")
    outputTables <- unique(c(
      outputTables,
      "cohortInclusionTable",
      "cohortInclusionResultTable"
    ))
  }

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
      includeDatabaseId = includeDatabaseId,
      databaseId = databaseId
    )
  }
  if ("cohortAttritionTable" %in% requestedTables) {
    if (is.null(inclusionRules)) {
      inclusionRules <- results$cohortInclusionTable
    }
    results$cohortAttritionTable <- computeCohortAttrition(
      cohortInclusionResult = results$cohortInclusionResultTable,
      cohortInclusion = inclusionRules
    )
    if (isFALSE(snakeCaseToCamelCase)) {
      names(results$cohortAttritionTable) <- SqlRender::camelCaseToSnakeCase(names(results$cohortAttritionTable))
    }
  }

  if (!("cohortInclusionTable" %in% requestedTables)) {
    results$cohortInclusionTable <- NULL
  }
  if (!("cohortInclusionResultTable" %in% requestedTables)) {
    results$cohortInclusionResultTable <- NULL
  }

  return(results)
}


#' Compute cohort attrition from inclusion rule statistics
#'
#' @description
#' Computes a sequential attrition table using the inclusion
#' rule statistics stored in the cohort statistics tables for Circe-based
#' cohorts. For each cohort definition, we report a base cohort entry count
#' (before inclusion rules) and then counts after applying the first
#' \code{k} inclusion rules in sequence.
#'
#' Inclusion rule satisfaction is encoded as a bit mask in
#' \code{inclusionRuleMask}. For a rule sequence \code{i}, its bit value is
#' \code{2^i}. A row with \code{inclusionRuleMask} equal to the sum of the
#' bits indicates which rules were met. To compute the count after the first
#' \code{k} rules, we require all first-\code{k} bits to be set by checking
#' \code{bitwAnd(inclusionRuleMask, requiredMask) == requiredMask}, where
#' \code{requiredMask = 2^k - 1}.
#'
#' Attrition is computed separately for each \code{modeId} present in
#' \code{cohortInclusionResult} (for example, person-level and event-level).
#'
#' @param cohortInclusionResult A data.frame containing inclusion rule masks
#' and counts, typically from the \code{cohortInclusionResultTable} with
#' camelCase column names.
#' Required columns: \code{databaseId}, \code{cohortDefinitionId},
#' \code{inclusionRuleMask}, \code{modeId}, \code{personCount}.
#' You can obtain this via \code{getCohortStats(..., outputTables = "cohortInclusionResultTable")}
#' or by querying the cohort results schema table created when stats are generated.
#'
#' @param cohortInclusion A data.frame of inclusion rule metadata, typically
#' from \code{cohortInclusionTable} with camelCase column names.
#' Required columns: \code{cohortDefinitionId}, \code{ruleSequence}.
#' You can obtain this via \code{getCohortStats(..., outputTables = "cohortInclusionTable")}
#' or by querying the cohort results schema table created when stats are generated.
#'
#' @return A data.frame with the following columns:
#' \itemize{
#'   \item \code{databaseId}: Database identifier.
#'   \item \code{cohortDefinitionId}: Cohort definition identifier.
#'   \item \code{modeId}: The mode identifier from \code{cohortInclusionResult}.
#'   \item \code{cohortEntry}: 1 for the base cohort entry count, 0 for rule rows.
#'   \item \code{ruleSequence}: Inclusion rule sequence (-1 for base row).
#'   \item \code{personCount}: Count after applying rules.
#' }
#'
#' @export
computeCohortAttrition <- function(cohortInclusionResult,
                                   cohortInclusion) {
  checkmate::assert_data_frame(cohortInclusionResult)
  checkmate::assert_data_frame(cohortInclusion)

  # Force all columns to camelCase
  if (!all(isCamelCase(names(cohortInclusionResult)))) {
    names(cohortInclusionResult) <- SqlRender::snakeCaseToCamelCase(names(cohortInclusionResult))
  }
  if (!all(isCamelCase(names(cohortInclusion)))) {
    names(cohortInclusion) <- SqlRender::snakeCaseToCamelCase(names(cohortInclusion))
  }

  # Add the databaseId column if it is missing since
  # this is requried later in the function
  if (!"databaseId" %in% names(cohortInclusionResult)) {
    cohortInclusionResult <- cohortInclusionResult |>
      dplyr::mutate(databaseId = NA)
  }
  cohortInclusionResultRequiredColumns <- c(
    "databaseId",
    "cohortDefinitionId",
    "inclusionRuleMask",
    "modeId",
    "personCount"
  )
  missingColumns <- setdiff(cohortInclusionResultRequiredColumns, names(cohortInclusionResult))
  if (length(missingColumns) > 0) {
    stop(paste("Missing required columns in cohortInclusionResult:", paste(missingColumns, collapse = ", ")))
  }

  cohortInclusionRequiredColumns <- c(
    "ruleSequence",
    "cohortDefinitionId"
  )
  missingColumns <- setdiff(cohortInclusionRequiredColumns, names(cohortInclusion))
  if (length(missingColumns) > 0) {
    stop(paste("Missing required columns in cohortInclusion:", paste(missingColumns, collapse = ", ")))
  }

  result <- cohortInclusionResult

  emptyColumns <- c(
    "databaseId",
    "cohortDefinitionId",
    "modeId",
    "cohortEntry",
    "ruleSequence",
    "personCount"
  )

  if (nrow(result) == 0) {
    empty <- as.data.frame(setNames(replicate(length(emptyColumns), logical(0), simplify = FALSE), emptyColumns))
    return(empty)
  }

  base <- result %>%
    dplyr::group_by(.data$databaseId, .data$cohortDefinitionId, .data$modeId) %>%
    dplyr::summarise(personCount = sum(.data$personCount, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      cohortEntry = 1L,
      ruleSequence = as.integer(-1)
    )

  rules <- cohortInclusion %>%
    dplyr::select(.data$cohortDefinitionId, .data$ruleSequence) %>%
    dplyr::distinct() %>%
    dplyr::mutate(requiredMask = 2^(.data$ruleSequence + 1) - 1)

  ruleRows <- result %>%
    dplyr::inner_join(rules, by = "cohortDefinitionId", relationship = "many-to-many") %>%
    dplyr::filter(bitwAnd(.data$inclusionRuleMask, .data$requiredMask) == .data$requiredMask) %>%
    dplyr::group_by(.data$databaseId, .data$cohortDefinitionId, .data$modeId, .data$ruleSequence) %>%
    dplyr::summarise(personCount = sum(.data$personCount, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(
      cohortEntry = 0L
    )

  cohortModes <- result %>%
    dplyr::select(.data$databaseId, .data$cohortDefinitionId, .data$modeId) %>%
    dplyr::distinct()
  zeroCountRuleRows <- cohortModes %>%
    dplyr::inner_join(rules, by = "cohortDefinitionId", relationship = "many-to-many") %>%
    dplyr::select(.data$databaseId, .data$cohortDefinitionId, .data$modeId, .data$ruleSequence) %>%
    dplyr::left_join(ruleRows,
      by = c(
        "databaseId",
        "cohortDefinitionId",
        "modeId",
        "ruleSequence"
      ),
      relationship = "many-to-many"
    ) %>%
    dplyr::filter(is.na(.data$personCount)) %>%
    dplyr::mutate(
      cohortEntry = 0L,
      personCount = 0L
    )

  output <- dplyr::bind_rows(base, ruleRows, zeroCountRuleRows) %>%
    dplyr::select(all_of(emptyColumns)) %>%
    dplyr::arrange(.data$cohortDefinitionId, .data$modeId, dplyr::desc(.data$cohortEntry), .data$ruleSequence)

  return(output)
}


#' Get Cohort Inclusion Rules from a cohort definition set
#'
#' @description
#' This function returns a data frame of the inclusion rules defined
#' in a cohort definition set.
#'
#' @md
#' @template CohortDefinitionSet
#'
#' @export
getCohortInclusionRules <- function(cohortDefinitionSet) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
    must.include = c(
      "cohortId",
      "cohortName",
      "json"
    )
  )

  # Assemble the cohort inclusion rules
  # NOTE: This data frame must match the @cohort_inclusion_table
  # structure as defined in inst/sql/sql_server/CreateCohortTables.sql
  inclusionRules <- data.frame(
    cohortDefinitionId = numeric(),
    ruleSequence = integer(),
    name = character(),
    description = character()
  )

  # Remove any cohort definitions that do not include the JSON property
  cohortDefinitionSet <- cohortDefinitionSet[!(is.null(cohortDefinitionSet$json) | is.na(cohortDefinitionSet$json)), ]
  for (i in 1:nrow(cohortDefinitionSet)) {
    cohortDefinition <- ParallelLogger::convertJsonToSettings(json = cohortDefinitionSet$json[i])
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
              cohortDefinitionId = as.numeric(cohortDefinitionSet$cohortId[i]),
              ruleSequence = as.integer(j - 1),
              name = ruleName,
              description = ruleDescription
            )
          )
        }
      }
    }
  }

  invisible(inclusionRules)
}
