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

#' Create an empty cohort definition set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{generateCohortSet}.
#'
#' @return
#' Returns an empty cohort set data.frame
#' 
#' @export
createEmptyCohortDefinitionSet <- function() {
  return(setNames(data.frame(matrix(ncol = 3, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortName", "sql")))
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
#' @param incremental                 Create only cohorts that haven't been created before?
#' 
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are
#'                                    kept of which definition has been executed.
#'
#' @export
generateCohortSet <- function(connectionDetails = NULL,
                              connection = NULL,
                              cdmDatabaseSchema,
                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTableNames = getCohortTableNames(),
                              cohortDefinitionSet = NULL,
                              incremental = FALSE,
                              incrementalFolder = NULL) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c("cohortId",
                                          "cohortName",
                                          "sql"))
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
    cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
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
                                                   cohortDefinitionSet$cohortId,
                                                   generateCohort,
                                                   cohortDefinitionSet = cohortDefinitionSet,
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
#' @param incremental       Create only cohorts that haven't been created before?
#' 
#' @param recordKeepingFile If \code{incremental = TRUE}, this file will contain
#'                          information on cohorts already generated
generateCohort <- function(cohortId = NULL,
                           cohortDefinitionSet,
                           connection = NULL,
                           connectionDetails = NULL,
                           cdmDatabaseSchema,
                           tempEmulationSchema,
                           cohortDatabaseSchema,
                           cohortTableNames,
                           incremental,
                           recordKeepingFile) {
  # Get the index of the cohort record for the current cohortId
  i <- which(cohortDefinitionSet$cohortId == cohortId)
  if (!incremental || isTaskRequired(cohortId = cohortDefinitionSet$cohortId[i],
                                     checksum = cohortDefinitionSet$checksum[i],
                                     recordKeepingFile = recordKeepingFile)) {
    if (is.null(connection)) {
      # Establish the connection and ensure the cleanup is performed
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }

    ParallelLogger::logInfo(i, "/", nrow(cohortDefinitionSet), "- Generating cohort: ", cohortDefinitionSet$cohortName[i])
    sql <- cohortDefinitionSet$sql[i]
    sql <- SqlRender::render(sql = sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             results_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTableNames$cohortTable,
                             target_cohort_id = cohortDefinitionSet$cohortId[i],
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
      recordTasksDone(cohortId = cohortDefinitionSet$cohortId[i],
                      checksum = cohortDefinitionSet$checksum[i],
                      recordKeepingFile = recordKeepingFile)
    }

    return(cohortDefinitionSet$cohortId[i])
  } else {
    return(NULL)
  }
}