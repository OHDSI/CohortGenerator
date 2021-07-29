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

#' Create cohort table(s)
#'
#' @description
#' This function creates an empty cohort table. Optionally, additional empty tables are created to
#' store statistics on the various inclusion criteria.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param createInclusionStatsTables   Create the four additional tables for storing inclusion rule
#'                                     statistics?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
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
#' @export
createCohortTable <- function(connectionDetails = NULL,
                              connection = NULL,
                              cohortDatabaseSchema,
                              cohortTable = "cohort",
                              createInclusionStatsTables = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"),
                              cohortCensorStatsTable = paste0(cohortTable, "_censor_stats")) {
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }

  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- loadRenderTranslateSql("CreateCohortTable.sql",
                                dbms = connection@dbms,
                                cohort_database_schema = cohortDatabaseSchema,
                                cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortTable)

  if (createInclusionStatsTables) {
    ParallelLogger::logInfo("Creating inclusion rule statistics tables")
    sql <- loadRenderTranslateSql("CreateInclusionStatsTables.sql",
                                  dbms = connection@dbms,
                                  cohort_database_schema = resultsDatabaseSchema,
                                  cohort_inclusion_table = cohortInclusionTable,
                                  cohort_inclusion_result_table = cohortInclusionResultTable,
                                  cohort_inclusion_stats_table = cohortInclusionStatsTable,
                                  cohort_summary_stats_table = cohortSummaryStatsTable,
                                  cohort_censor_stats_table = cohortCensorStatsTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionResultTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionStatsTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortSummaryStatsTable)
  }
  delta <- Sys.time() - start
  writeLines(paste("Creating cohort table took", round(delta, 2), attr(delta, "units")))
}

#' Create an empty cohort set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{instantiateCohortSet}.
#'
#' @return
#' Returns an empty cohort set data.frame
#' 
#' @export
createEmptyCohortSet <- function() {
  return(setNames(data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = FALSE), c("cohortId","cohortFullName", "sql", "json")))
}

#' Instantiate a set of cohorts
#'
#' @description
#' This function instantiates a set of cohorts in the cohort table and where
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
instantiateCohortSet <- function(connectionDetails = NULL,
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
    recordKeepingFile <- file.path(incrementalFolder, "InstantiatedCohorts.csv")
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
  writeLines(paste("Instantiating cohort set took", round(delta, 2), attr(delta, "units")))
  return(cohortsGenerated)
}

#' Generates a cohort
#'
#' @description
#' This function is used by \code{instantiateCohortSet} to generate a cohort
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
    ParallelLogger::logInfo(i,
                            "/",
                            nrow(cohortSet),
                            ": Instantiation cohort ",
                            cohortSet$cohortFullName[i])
    sql <- cohortSet$sql[i]
    generateInclusionStats <- sqlContainsInclusionRuleStats(sql)
    if (generateInclusionStats) {
      createTempInclusionStatsTables(connection, tempEmulationSchema, cohortSet$json[i], cohortId)
    }
    
    if (generateInclusionStats) {
      sql <- SqlRender::render(sql,
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
      sql <- SqlRender::render(sql,
                               cdm_database_schema = cdmDatabaseSchema,
                               vocabulary_database_schema = cdmDatabaseSchema,
                               target_database_schema = cohortDatabaseSchema,
                               target_cohort_table = cohortTable,
                               target_cohort_id = cohortSet$cohortId[i])
    }
    sql <- SqlRender::translate(sql,
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
  sql <- loadRenderTranslateSql("CreateInclusionStatsTempTables.sql",
                                dbms = connection@dbms,
                                tempEmulationSchema = tempEmulationSchema)
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
#' @param sql   A string containing the SQL used to instantiate a cohort. This
#'              code assumes that the SQL has not been rendered using SqlRender
#'              in order to detect tokens that indicate the generation of
#'              inclusion rule statistics in addition to the cohort.
sqlContainsInclusionRuleStats <- function(sql) {
  sql <- SqlRender::render(sql, warnOnMissingParameters = FALSE)
  return (grepl("(@results_database_schema.cohort_censor_stats)", sql))
}
