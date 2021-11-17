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
  return(setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "sql", "json")))
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
                              numThreads = 1,
                              cdmDatabaseSchema,
                              tempEmulationSchema = NULL,
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTable = "cohort",
                              cohortSet = NULL,
                              inclusionStatisticsFolder = NULL,
                              createCohortTable = FALSE,
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
  if (numThreads != 1) {
    stop("numThreads must be set to 1 for now.")
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (createCohortTable) {
    needToCreate <- TRUE
    if (incremental) {
      tables <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
      if (toupper(cohortTable) %in% toupper(tables)) {
        ParallelLogger::logInfo("Cohort table already exists and in incremental mode, so not recreating table.")
        needToCreate <- FALSE
      }
    }
    if (needToCreate) {
      createCohortTable(connection = connection,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        cohortTable = cohortTable,
                        createInclusionStatsTables = FALSE)
    }
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
  cluster <- ParallelLogger::makeCluster(numberOfThreads = numThreads)
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
                                                   cohortTable = cohortTable,
                                                   inclusionStatisticsFolder = inclusionStatisticsFolder,
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
                           cohortTable,
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

    # Log the operation
    ParallelLogger::logInfo(i, "/", nrow(cohortSet), "- Generating cohort: ", cohortSet$cohortFullName[i])
    sql <- cohortSet$sql[i]
    generateInclusionStats <- sqlContainsInclusionRuleStats(sql)
    if (generateInclusionStats) {
      createTempInclusionStatsTables(connection, tempEmulationSchema, cohortSet$json[i], cohortId)
    }
    
    if (generateInclusionStats) {
      sql <- SqlRender::render(sql = sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohortSet$cohortId[i],
                               results_database_schema.cohort_inclusion = "#cohort_inclusion",
                               results_database_schema.cohort_inclusion_result = "#cohort_inc_result",
                               results_database_schema.cohort_inclusion_stats = "#cohort_inc_stats",
                               results_database_schema.cohort_summary_stats = "#cohort_summary_stats",
                               results_database_schema.cohort_censor_stats = "#cohort_censor_stats")
    } else {
      sql <- SqlRender::render(sql = sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohortSet$cohortId[i])
    }
    sql <- SqlRender::translate(sql = sql,
                                targetDialect = connectionDetails$dbms,
                                tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection, sql)

    if (generateInclusionStats && !is.null(inclusionStatisticsFolder)) {
      saveAndDropTempInclusionStatsTables(connection = connection,
                                          tempEmulationSchema = tempEmulationSchema,
                                          inclusionStatisticsFolder = inclusionStatisticsFolder,
                                          incremental = incremental,
                                          cohortIds = cohortSet$cohortId[i])
    }

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

createTempInclusionStatsTables <- function(connection, tempEmulationSchema, cohortJson, cohortId) {
  ParallelLogger::logInfo("Creating temporary inclusion statistics tables")
  sql <- SqlRender::readSql(system.file("sql/sql_server/CreateInclusionStatsTempTables.sql", package = "CohortGenerator", mustWork = TRUE))
  sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms, tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql)

  inclusionRules <- data.frame(cohortDefinitionId = as.double(),
                               ruleSequence = as.integer(),
                               name = as.character())

  cohortDefinition <- RJSONIO::fromJSON(content = cohortJson, digits = 23)
  if (!is.null(cohortDefinition$InclusionRules)) {
    nrOfRules <- length(cohortDefinition$InclusionRules)
    if (nrOfRules > 0) {
      for (j in 1:nrOfRules) {
        inclusionRules <- rbind(inclusionRules, data.frame(cohortDefinitionId = cohortId,
                                                           ruleSequence = j - 1,
                                                           name = cohortDefinition$InclusionRules[[j]]$name))
      }
    }
  }
  
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#cohort_inclusion",
                                 data = inclusionRules,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 tempEmulationSchema = tempEmulationSchema,
                                 camelCaseToSnakeCase = TRUE)
}

saveAndDropTempInclusionStatsTables <- function(connection,
                                                tempEmulationSchema,
                                                inclusionStatisticsFolder,
                                                incremental,
                                                cohortIds) {
  fetchStats <- function(table, fileName, cohortIds) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @table"
    data <- DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                                       connection = connection,
                                                       tempEmulationSchema = tempEmulationSchema,
                                                       snakeCaseToCamelCase = TRUE,
                                                       table = table)
    fullFileName <- file.path(inclusionStatisticsFolder, fileName)
    if(nrow(data) > 0) {
      if (incremental) {
        saveIncremental(data, fullFileName, cohortDefinitionId = cohortIds)
      } else {
        readr::write_csv(x = data, file = fullFileName)
      }
    }
  }
  fetchStats("#cohort_inclusion", "cohortInclusion.csv", cohortIds)
  fetchStats("#cohort_inc_result", "cohortIncResult.csv", cohortIds)
  fetchStats("#cohort_inc_stats", "cohortIncStats.csv", cohortIds)
  fetchStats("#cohort_summary_stats", "cohortSummaryStats.csv", cohortIds)
  fetchStats("#cohort_censor_stats", "cohortCensorStats.csv", cohortIds)
  
  sql <- "TRUNCATE TABLE #cohort_inclusion;
  DROP TABLE #cohort_inclusion;

  TRUNCATE TABLE #cohort_inc_result;
  DROP TABLE #cohort_inc_result;

  TRUNCATE TABLE #cohort_inc_stats;
  DROP TABLE #cohort_inc_stats;

  TRUNCATE TABLE #cohort_summary_stats;
  DROP TABLE #cohort_summary_stats;
  
  TRUNCATE TABLE #cohort_censor_stats;
  DROP TABLE #cohort_censor_stats;"
  
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE,
                                               tempEmulationSchema = tempEmulationSchema)
}

#' Detects if the SQL indicate to compute inclusion rule statistics
#'
#' @description
#' This function takes as a parameter a SQL script used to generate a cohort
#' and performs a string search for tokens that indicate to generate the inclusion 
#' statistics. This SQL is usually generated by circe-be.
#' 
#' This function also assumes that the SQL passed into the function has not been
#' translated to a specific SQL dialect. 
#'
#' @param sql   A string containing the SQL used to generate the cohort. This
#'              code assumes that the SQL has not been rendered using SqlRender
#'              in order to detect tokens that indicate the generation of
#'              inclusion rule statistics in addition to the cohort.
sqlContainsInclusionRuleStats <- function(sql) {
  sql <- SqlRender::render(sql = sql, warnOnMissingParameters = FALSE)
  hasCohortCensorStatsTable <- grepl("(@results_database_schema.cohort_censor_stats)", sql)
  hasOtherInclusionRuleTables <- grepl("(@results_database_schema.cohort_inclusion_result)", sql) &&
    grepl("(@results_database_schema.cohort_inclusion_stats)", sql) &&
    grepl("(@results_database_schema.cohort_summary_stats)", sql)
  return (hasCohortCensorStatsTable || hasOtherInclusionRuleTables)
}
