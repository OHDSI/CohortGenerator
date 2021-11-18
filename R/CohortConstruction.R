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

#' Create an empty cohort set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{generateCohortSet}.
#'
#' @return
#' Returns an empty cohort set data.frame
#' 
#' @export
createEmptyCohortSet <- function() {
  return(setNames(data.frame(matrix(ncol = 3, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "sql")))
}

#' Generate a set of cohorts
#'
#' @description
#' This function generates a set of cohorts in the cohort table and where
#' specified the inclusion rule statistics are computed and stored in the
#' \code{inclusionStatisticsFolder}.
#'
#' @template Connection
#'
#' @param numThreads                  Specify the number of threads for cohort generation. Currently
#'                                    this only supports single threaded operations.
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @template CohortSet
#'
#' @template InclusionStatisticsFolder
#' 
#' @param createCohortTable           Create the cohort table? If \code{incremental = TRUE} and the
#'                                    table already exists this will be skipped.
#' @param incremental                 Create only cohorts that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#'
#' @export
generateCohortSet <- function(connectionDetails = NULL,
                              connection = NULL,
                              cdmDatabaseSchema,
                              tempEmulationSchema = NULL,
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTableNames = getCohortTableNames(),
                              cohortSet = NULL,
                              incremental = FALSE,
                              incrementalFolder = NULL) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  if (!is.null(cohortSet) & is.data.frame(cohortSet)) {
    cohortRequiredColumns <- colnames(createEmptyCohortSet())
    if (length(intersect(names(cohortSet), cohortRequiredColumns)) != length(cohortRequiredColumns)) {
      stop(paste("The cohortSet data frame must contain the following columns:", cohortRequiredColumns, sep = ","))
    }
  } else {
    stop("The cohortSet parameter is mandatory and must be a data frame.")
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  # This is for when we parallel cohort generation
  #if (numThreads < 1 || numThreads > parallel::detectCores()) {
  #  stop(paste0("The numThreads argument must be between 1 and", parallel::detectCores()))
  #}

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  # Verify the cohort tables exist and if they do not
  # stop the generation process
  tableExistsFlagList <- lapply(cohortTableNames, FUN=function(x) { x = FALSE })
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
    cohortSet$checksum <- computeChecksum(cohortSet$sql)
    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohorts.csv")
  }

  # Revisit parallel generation later
  # if (numThreads > 1) {
  #   cluster <- ParallelLogger::logInfo(paste0("Generating cohorts in parallel using ",
  #                                             numThreads,
  #                                             " threads. Individual cohort generation progress will not be displayed in the console."))
  # }

  # Create the cluster
  cluster <- ParallelLogger::makeCluster(numberOfThreads = 1)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)

  # Apply the generation operation to the cluster
  cohortsGenerated <- ParallelLogger::clusterApply(cluster,
                                                   cohortSet$cohortId,
                                                   generateCohort,
                                                   cohortSet = cohortSet,
                                                   connection = connection,
                                                   connectionDetails = connectionDetails,
                                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                                   tempEmulationSchema = tempEmulationSchema,
                                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                                   cohortTableNames = cohortTableNames,
                                                   incremental = incremental,
                                                   recordKeepingFile = recordKeepingFile,
                                                   stopOnError = TRUE,
                                                   progressBar = TRUE)

  delta <- Sys.time() - start
  writeLines(paste("Generating cohort set took", round(delta, 2), attr(delta, "units")))
  return(cohortsGenerated)
}

#' Generates a cohort
#'
#' @description
#' This function is used by \code{generateCohortSet} to generate a cohort
#' against the CDM.
#'
#' @param cohortId   The cohortId in the list of \code{cohortSet}
#' 
#' @template CohortSet
#' 
#' @template Connection
#' 
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTable
#' 
#' @template InclusionStatisticsFolder
#' 
#' @param incremental       Create only cohorts that haven't been created before?
#' 
#' @param recordKeepingFile If \code{incremental = TRUE}, this file will contain
#'                          information on cohorts already generated
generateCohort <- function(cohortId = NULL,
                           cohortSet,
                           connection = NULL,
                           connectionDetails = NULL,
                           cdmDatabaseSchema,
                           tempEmulationSchema,
                           cohortDatabaseSchema,
                           cohortTableNames,
                           inclusionStatisticsFolder,
                           incremental,
                           recordKeepingFile) {
  # Get the index of the cohort record for the current cohortId
  i <- which(cohortSet$cohortId == cohortId)
  if (!incremental || isTaskRequired(cohortId = cohortSet$cohortId[i],
                                     checksum = cohortSet$checksum[i],
                                     recordKeepingFile = recordKeepingFile)) {
    if (is.null(connection)) {
      # Establish the connection and ensure the cleanup is performed
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }

    ParallelLogger::logInfo(i, "/", nrow(cohortSet), "- Generating cohort: ", cohortSet$cohortFullName[i])
    sql <- cohortSet$sql[i]
    sql <- SqlRender::render(sql = sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             results_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTableNames$cohortTable,
                             target_cohort_id = cohortSet$cohortId[i],
                             results_database_schema.cohort_inclusion = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionTable, sep="."),
                             results_database_schema.cohort_inclusion_result = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionResultTable, sep="."),
                             results_database_schema.cohort_inclusion_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionStatsTable, sep="."),
                             results_database_schema.cohort_summary_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortSummaryStatsTable, sep="."),
                             results_database_schema.cohort_censor_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortCensorStatsTable, sep="."),
                             warnOnMissingParameters = FALSE)
    sql <- SqlRender::translate(sql = sql,
                                targetDialect = connectionDetails$dbms,
                                tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection, sql)

    if (incremental) {
      recordTasksDone(cohortId = cohortSet$cohortId[i],
                      checksum = cohortSet$checksum[i],
                      recordKeepingFile = recordKeepingFile)
    }

    return(cohortSet$cohortId[i])
  } else {
    return(NULL)
  }
}