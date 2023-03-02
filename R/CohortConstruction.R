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

  # Verify the cohort tables exist and if they do not
  # stop the generation process
  tableExistsFlagList <- lapply(cohortTableNames, FUN = function(x) {
    x <- FALSE
  })
  tables <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
  for (i in 1:length(cohortTableNames)) {
    if (toupper(cohortTableNames[i]) %in% toupper(tables)) {
      tableExistsFlagList[i] <- TRUE
    }
  }

  if (!all(unlist(tableExistsFlagList, use.names = FALSE))) {
    errorMsg <- "The following tables have not been created: \n"
    for (i in 1:length(cohortTableNames)) {
      if (!tableExistsFlagList[[i]]) {
        errorMsg <- paste0(errorMsg, "   - ", cohortTableNames[i], "\n")
      }
    }
    errorMsg <- paste(errorMsg, "Please use the createCohortTables function to ensure all tables exist before generating cohorts.", sep = "\n")
    stop(errorMsg)
  }


  if (incremental) {
    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")

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
  }
  # Create the cluster
  # DEV NOTE :: running subsets in a multiprocess setup will not work with subsets that subset other subsets
  # To resolve this issue we need to execute the dependency tree.
  # This could be done in an manner where all dependent cohorts are guaranteed to be sent to the same process and
  # the execution is in order. If you set numberOfThreads > 1 you should implement this!
  cluster <- ParallelLogger::makeCluster(numberOfThreads = 1)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)
  cohortsToGenerate <- cohortDefinitionSet$cohortId
  subsetsToGenerate <- c()
  # Generate top level cohorts first
  if (isTRUE(attr(cohortDefinitionSet, "hasSubsetDefinitions"))) {
    cohortsToGenerate <- cohortDefinitionSet %>%
      dplyr::filter(!.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()

    subsetsToGenerate <- cohortDefinitionSet %>%
      dplyr::filter(.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()
  }

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
  cohortsGenerated <- do.call(rbind, c(cohortsGenerated, subsetsGenerated))

  delta <- Sys.time() - start
  writeLines(paste("Generating cohort set took", round(delta, 2), attr(delta, "units")))
  invisible(cohortsGenerated)
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

  if (!incremental || isTaskRequired(
    cohortId = cohortDefinitionSet$cohortId[i],
    checksum = cohortDefinitionSet$checksum[i],
    recordKeepingFile = recordKeepingFile
  )) {
    if (is.null(connection)) {
      # Establish the connection and ensure the cleanup is performed
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    ParallelLogger::logInfo(i, "/", nrow(cohortDefinitionSet), "- Generating cohort: ", cohortName)
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
        warnOnMissingParameters = FALSE
      )
    }
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    # Helper function used within the tryCatch block below
    runCohortSql <- function(sql, startTime, incremental, cohortId, checksum, recordKeepingFile) {
      DatabaseConnector::executeSql(connection, sql)
      endTime <- lubridate::now()

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

    # This syntax is strange so leaving a note.
    # generationInfo is assigned based on the evaluation of
    # the expression in the tryCatch(). If there is an error, the
    # outermost assignment will assign generationInfo based on the return
    # value in the error() block. If the expr() function evaluates without
    # error, the inner most assignment of generationInfo will take place.
    generationInfo <- tryCatch(expr = {
      startTime <- lubridate::now()
      generationInfo <- runCohortSql(
        sql = sql,
        startTime = startTime,
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
  } else {
    generationInfo <- list(
      generationStatus = "SKIPPED",
      startTime = NA,
      endTime = NA
    )
  }

  summary <- data.frame(
    cohortId = cohortId,
    cohortName = cohortName,
    generationStatus = generationInfo$generationStatus,
    startTime = generationInfo$startTime,
    endTime = generationInfo$endTime
  )
  return(summary)
}
