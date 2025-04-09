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


#' Get last generated cohort checksums
#'
#' @description
#' This gets a log of the last checksum for each cohort id stored in the cohort_checksum table.
#'
#' This should be used to audit cohort generation as (if generated with cohort_generator) cohorts should always have an
#' end time in this table. The last end time will be the cohort that is in the cohort table (assuming no other manual
#' modifications are made to the cohort table itself).
#'
#' This can be used downstream of CohortGeneratro to evaluate if cohorts are consistent with passed definitions.
#'
#' @inheritParams generateCohortSet
#' 
#' @param cohortId cohortId to check. If NULL, all cohorts will be returned.
#' @param .checkTables used internally
getLastGeneratedCohortChecksums <- function(connectionDetails = NULL,
                                            connection = NULL,
                                            cohortId = NULL,
                                            cohortDatabaseSchema,
                                            cohortTableNames = getCohortTableNames(),
                                            .checkTables = TRUE) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  .checkCohortTables(connection, cohortDatabaseSchema, cohortTableNames)
  sql <- "
  WITH ranked_times AS (
      SELECT
          cohort_definition_id,
          checksum,
          start_time,
          end_time,
          ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY end_time DESC) AS rank
      FROM
          @cohort_database_schema.@cohort_checksum_table
     WHERE end_time IS NOT NULL
  )

  SELECT cohort_definition_id, checksum, start_time, end_time 
  FROM ranked_times 
  WHERE rank = 1;
  {@cohort_id != ''} ? {AND cohort_definition_id IN (@cohort_id)}
  "

  results <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                        sql = sql,
                                                        cohort_id = cohortId,
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_checksum_table = cohortTableNames$cohortChecksumTable,
                                                        snakeCaseToCamelCase = TRUE)

  results$startTime <- as.POSIXct(results$startTime/1000, origin = "1970-01-01", tz = "UTC")
  results$endTime <- as.POSIXct(results$endTime/1000, origin = "1970-01-01", tz = "UTC")

  return(results)
}


#' Generate a set of cohorts
#'
#' @description
#' This function generates a set of cohorts in the cohort table.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTableNames
#'
#' @template CohortDefinitionSet
#'
#' @param stopOnError                 If an error happens while generating one of the cohorts in the
#'                                    cohortDefinitionSet, should we stop processing the other
#'                                    cohorts? The default is TRUE; when set to FALSE, failures will
#'                                    be identified in the return value from this function.
#'
#' @param incremental                 Create only cohorts that haven't been created before?
#'
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#' @returns
#'
#' A data.frame consisting of the following columns:
#' \describe{
#'    \item{cohortId}{The unique integer identifier of the cohort}
#'    \item{cohortName}{The cohort's name}
#'    \item{generationStatus}{The status of the generation task which may be one of the following:
#'           \describe{
#'                \item{COMPLETE}{The generation completed successfully}
#'                \item{FAILED}{The generation failed (see logs for details)}
#'                \item{SKIPPED}{If using incremental == 'TRUE', this status indicates
#'                               that the cohort's generation was skipped since it
#'                               was previously completed.}
#'           }}
#'    \item{startTime}{The start time of the cohort generation. If the generationStatus == 'SKIPPED', the startTime will be NA.}
#'    \item{endTime}{The end time of the cohort generation. If the generationStatus == 'FAILED', the endTime will be the time of the failure.
#'                   If the generationStatus == 'SKIPPED', endTime will be NA.}
#'    }
#'
#' @export
generateCohortSet <- function(connectionDetails = NULL,
                              connection = NULL,
                              cdmDatabaseSchema,
                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTableNames = getCohortTableNames(),
                              cohortDefinitionSet = NULL,
                              stopOnError = TRUE,
                              incremental = FALSE,
                              incrementalFolder = NULL) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )
  assertLargeInteger(cohortDefinitionSet$cohortId)
  # Verify that cohort IDs are not repeated in the cohort definition
  # set before generating
  if (length(unique(cohortDefinitionSet$cohortId)) != length(cohortDefinitionSet$cohortId)) {
    duplicatedCohortIds <- cohortDefinitionSet$cohortId[duplicated(cohortDefinitionSet$cohortId)]
    stop("Cannot generate! Duplicate cohort IDs found in your cohortDefinitionSet: ", paste(duplicatedCohortIds, sep = ","), ". Please fix your cohortDefinitionSet and try again.")
  }
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  .checkCohortTables(connection, cohortDatabaseSchema, cohortTableNames)

  if (isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))) {
    cohortDefinitionSet$checksum <- ""
    for (i in 1:nrow(cohortDefinitionSet)) {
      # This implementation supports recursive definitions (subsetting subsets) because the subsets have to be added in order
      if (cohortDefinitionSet$subsetParent[i] != cohortDefinitionSet$cohortId[i]) {
        j <- which(cohortDefinitionSet$cohortId == cohortDefinitionSet$subsetParent[i])
        cohortDefinitionSet$checksum[i] <- computeChecksum(paste(
          cohortDefinitionSet$sql[j],
          cohortDefinitionSet$sql[i]
        ))
      } else {
        cohortDefinitionSet$checksum[i] <- computeChecksum(cohortDefinitionSet$sql[i])
      }
    }
  } else {
    cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
  }

  if (incremental) {
    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
    computedChecksums <- getLastGeneratedCohortChecksums(connection = connection,
                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                         cohortTableNames = cohortTableNames) |>
      dplyr::rename(lastChecksum = "checksum")

    uncomputedCohorts <- cohortDefinitionSet |>
      dplyr::left_join(computedChecksums, by = c("cohortId" = "cohortDefinitionId")) |>
      dplyr::filter(.data$checksum != .data$lastChecksum | is.na(.data$lastChecksum)) |> # only compute items where the stored checksum differs
      dplyr::select(dplyr::all_of(colnames(cohortDefinitionSet)))

    computedCohorts <- cohortDefinitionSet |>
      dplyr::left_join(computedChecksums, by = c("cohortId" = "cohortDefinitionId")) |>
      dplyr::filter(.data$checksum == .data$lastChecksum) |>
      dplyr::select("cohortId", "cohortName", "checksum", "startTime", "endTime") |>
      dplyr::mutate(generationStatus = "SKIPPED")

    computedStr <- paste(computedCohorts$cohortId, collapse = ', ')
      ParallelLogger::logInfo(paste("Skipping cohorts already generated: ", computedStr))
  } else {
    uncomputedCohorts <- cohortDefinitionSet
    computedCohorts <- data.frame()
  }

  cohortsToGenerate <- uncomputedCohorts$cohortId
  subsetsToGenerate <- c()
  # Generate top level cohorts first
  if (isTRUE(attr(uncomputedCohorts, "hasSubsetDefinitions"))) {
    cohortsToGenerate <- uncomputedCohorts %>%
      dplyr::filter(!.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()

    subsetsToGenerate <- uncomputedCohorts %>%
      dplyr::filter(.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()
  }

  # Create the cluster
  # DEV NOTE :: running subsets in a multiprocess setup will not work with subsets that subset other subsets
  # To resolve this issue we need to execute the dependency tree.
  # This could be done in an manner where all dependent cohorts are guaranteed to be sent to the same process and
  # the execution is in order. If you set numberOfThreads > 1 you should implement this!
  cluster <- ParallelLogger::makeCluster(numberOfThreads = 1)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)

  # Apply the generation operation to the cluster
  cohortsGenerated <- ParallelLogger::clusterApply(
    cluster,
    cohortsToGenerate,
    generateCohort,
    cohortDefinitionSet = cohortDefinitionSet,
    connection = connection,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    stopIfError = stopOnError,
    incremental = incremental,
    recordKeepingFile = recordKeepingFile,
    stopOnError = stopOnError,
    progressBar = TRUE
  )
  subsetsGenerated <- c()
  if (length(subsetsToGenerate)) {
    subsetsGenerated <- ParallelLogger::clusterApply(
      cluster,
      subsetsToGenerate,
      generateCohort,
      cohortDefinitionSet = cohortDefinitionSet,
      connection = connection,
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      stopIfError = stopOnError,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      stopOnError = stopOnError,
      progressBar = TRUE
    )
  }

  # Convert the list to a data frame
  cohortsGenerated <- dplyr::bind_rows(cohortsGenerated, subsetsGenerated, computedCohorts)

  delta <- Sys.time() - start
  writeLines(paste("Generating cohort set took", round(delta, 2), attr(delta, "units")))
  invisible(cohortsGenerated)
}

# Helper function used within the tryCatch block below
.runCohortSql <- function(connection, sql, startTime, resultsDatabaseSchema, cohortChecksumTable, incremental, cohortId, checksum, recordKeepingFile) {
  startSql <- "DELETE FROM @results_database_schema.@cohort_checksum_table WHERE cohort_definition_id = @target_cohort_id AND checksum = '@checksum';
      INSERT INTO @results_database_schema.@cohort_checksum_table (cohort_definition_id, checksum, start_time, end_time) VALUES (@target_cohort_id, '@checksum', @start_time, NULL);"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               startSql,
                                               results_database_schema = resultsDatabaseSchema,
                                               cohort_checksum_table = cohortChecksumTable,
                                               target_cohort_id = cohortId,
                                               checksum = checksum,
                                               start_time = as.numeric(Sys.time()) * 1000,
                                               reportOverallTime = FALSE,
                                               progressBar = FALSE)
  DatabaseConnector::executeSql(connection, sql)
  endSql <- "
      UPDATE @results_database_schema.@cohort_checksum_table
      SET end_time = @end_time
      WHERE cohort_definition_id = @target_cohort_id
      AND checksum = '@checksum';"

  endTime <- lubridate::now()
  DatabaseConnector::renderTranslateExecuteSql(connection, endSql,
                                               target_cohort_id = cohortId,
                                               results_database_schema = resultsDatabaseSchema,
                                               cohort_checksum_table = cohortChecksumTable,
                                               checksum = checksum,
                                               end_time = as.numeric(Sys.time()) * 1000,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)

  if (incremental) {
    recordTasksDone(
      cohortId = cohortId,
      checksum = checksum,
      recordKeepingFile = recordKeepingFile
    )
  }

  return(list(
    generationStatus = "COMPLETE",
    startTime = startTime,
    endTime = endTime
  ))
}

#' Generates a cohort
#'
#' @description
#' This function is used by \code{generateCohortSet} to generate a cohort
#' against the CDM.
#'
#' @param cohortId   The cohortId in the list of \code{cohortDefinitionSet}
#'
#' @template cohortDefinitionSet
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTableNames
#'
#' @param stopIfError       When set to true, an error in processing will call
#'                          the stop() command to notify the parent calling
#'                          function that an error occurred.
#'
#' @param incremental       Create only cohorts that haven't been created before?
#'
#' @param recordKeepingFile If \code{incremental = TRUE}, this file will contain
#'                          information on cohorts already generated
#' @noRd
#' @keywords internal
generateCohort <- function(cohortId = NULL,
                           cohortDefinitionSet,
                           connection = NULL,
                           connectionDetails = NULL,
                           cdmDatabaseSchema,
                           tempEmulationSchema,
                           cohortDatabaseSchema,
                           cohortTableNames,
                           stopIfError = TRUE,
                           incremental,
                           recordKeepingFile) {
  # Get the index of the cohort record for the current cohortId
  i <- which(cohortDefinitionSet$cohortId == cohortId)
  cohortName <- cohortDefinitionSet$cohortName[i]
  isSubset <- FALSE
  if (isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))) {
    isSubset <- cohortDefinitionSet$isSubset[i]
  }

  
  if (is.null(connection)) {
    # Establish the connection and ensure the cleanup is performed
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  rlang::inform(paste0(i, "/", nrow(cohortDefinitionSet), "- Generating cohort: ", cohortName, " (id = ", cohortId, ")"))
  sql <- cohortDefinitionSet$sql[i]

  if (!isSubset) {
    sql <- SqlRender::render(
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = cdmDatabaseSchema,
      target_database_schema = cohortDatabaseSchema,
      results_database_schema = cohortDatabaseSchema,
      target_cohort_table = cohortTableNames$cohortTable,
      target_cohort_id = cohortDefinitionSet$cohortId[i],
      results_database_schema.cohort_inclusion = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionTable, sep = "."),
      results_database_schema.cohort_inclusion_result = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionResultTable, sep = "."),
      results_database_schema.cohort_inclusion_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionStatsTable, sep = "."),
      results_database_schema.cohort_summary_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortSummaryStatsTable, sep = "."),
      results_database_schema.cohort_censor_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortCensorStatsTable, sep = "."),
      warnOnMissingParameters = FALSE
    )
  } else {
    sql <- SqlRender::render(
      sql = sql,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_table = cohortTableNames$cohortTable,
      cohort_database_schema = cohortDatabaseSchema,
      checksum = cohortDefinitionSet$checksum[i],
      target_cohort_id = cohortDefinitionSet$cohortId[i],
      warnOnMissingParameters = FALSE
    )
  }
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  # This syntax is strange so leaving a note.
  # generationInfo is assigned based on the evaluation of
  # the expression in the tryCatch(). If there is an error, the
  # outermost assignment will assign generationInfo based on the return
  # value in the error() block. If the expr() function evaluates without
  # error, the inner most assignment of generationInfo will take place.
  generationInfo <- tryCatch(expr = {
    startTime <- lubridate::now()
    generationInfo <- .runCohortSql(
      connection = connection,
      sql = sql,
      startTime = startTime,
      resultsDatabaseSchema = cohortDatabaseSchema,
      cohortChecksumTable = cohortTableNames$cohortChecksumTable,
      incremental = incremental,
      cohortId = cohortDefinitionSet$cohortId[i],
      checksum = cohortDefinitionSet$checksum[i],
      recordKeepingFile = recordKeepingFile
    )
  }, error = function(e) {
    endTime <- lubridate::now()
    ParallelLogger::logError("An error occurred while generating cohortName = ", cohortName, ". Error: ", e)
    if (stopIfError) {
      stop()
    }
    return(list(
      generationStatus = "FAILED",
      startTime = startTime,
      endTime = endTime
    ))
  })
  

  summary <- data.frame(
    cohortId = cohortId,
    cohortName = cohortName,
    generationStatus = generationInfo$generationStatus,
    checksum = cohortDefinitionSet$checksum[i],
    startTime = generationInfo$startTime,
    endTime = generationInfo$endTime
  )
  return(summary)
}
