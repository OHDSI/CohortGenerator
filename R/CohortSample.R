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


#' Get sample set for a given cohort.
#' @description
#' Returns a set of integers of size n unless count is less than n, in which case it returns count integers
#'
#' @noRd
.getSampleSet <- function(connection,
                          n,
                          seed,
                          seedArgs,
                          cohortDatabaseSchema,
                          targetCohortId,
                          targetTable) {
  countSql <- "SELECT COUNT(DISTINCT SUBJECT_ID) as cnt FROM  @cohort_database_schema.@target_table
   WHERE cohort_definition_id = @target_cohort_id"
  count <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                      countSql,
                                                      cohort_database_schema = cohortDatabaseSchema,
                                                      target_cohort_id = targetCohortId,
                                                      target_table = targetTable) %>%
    dplyr::pull()

  if (count > n) {
    if (is.null(seedArgs)) {
      seedArgs <- list()
    }
    seedArgs$seed <- seed
    do.call(set.seed, seedArgs)
    return(data.frame(rand_id = sort(sample(1:count, n, replace = FALSE))))
  } else if (count == 0) {
    return(data.frame())
  }
  return(data.frame(rand_id = 1:count))
}

#' Sample cohort
#' @description
#' Samples a cohort with a specified integer set
#' @noRd
.sampleCohort <- function(connection,
                          targetCohortId,
                          targetTable,
                          outputCohortId,
                          outputTable,
                          cohortDatabaseSchema,
                          outputDatabaseSchema,
                          sampleTable,
                          seed,
                          tempEmulationSchema) {

  randSampleTableName <- paste0("#SAMPLE_TABLE_", seed)
  DatabaseConnector::insertTable(connection = connection,
                                 data = sampleTable,
                                 dropTableIfExists = TRUE,
                                 tempTable = TRUE,
                                 tempEmulationSchema = tempEmulationSchema,
                                 tableName = randSampleTableName)

  execSql <- SqlRender::readSql(system.file("sql", "sql_server", "sampling", "RandomSample.sql", package = "CohortGenerator"))
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               execSql,
                                               tempEmulationSchema = tempEmulationSchema,
                                               random_sample_table = randSampleTableName,
                                               target_cohort_id = targetCohortId,
                                               output_cohort_id = outputCohortId,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               output_database_schema = outputDatabaseSchema,
                                               output_table = outputTable,
                                               target_table = targetTable)
}


.computeIdentifierExpression <- function(identifierExpression, cohortId, seed) {
  allowed_vars <- c("cohortId", "seed")
  vars_in_expression <- intersect(all.vars(parse(text = identifierExpression)), allowed_vars)

  if (length(setdiff(all.vars(parse(text = identifierExpression)), vars_in_expression)) > 0) {
    stop("Invalid variable in expression.")
  }

  expr <- parse(text = identifierExpression)
  result <- eval(expr, list(cohortId = cohortId, seed = seed))
  return(result)
}

#' Sample Cohort Definition Set
#'
#' @description
#' Create 1 or more sample of size n of a cohort definition set
#'
#' Subsetted cohorts can be sampled, as with any other subset form.
#' However, subsetting a sampled cohort is not reccomended and not currently supported at this time.
#' In the case where n > cohort count the entire cohort is copied unmodified
#'
#' As different databases have different forms of randomness, the random selection is computed in
#' R, based on the count for each cohort. This is, therefore, db platform independent
#'
#' Note, this function assumes cohorts have already been generated.
#'
#' @param n                     Sample size
#' @param identifierExpression  Optional string R expression used to compute output cohort id. Can only use variables
#'                              cohortId and seed. Default is "cohortId * 1000 + seed", which is substituted and evaluated
#' @param cohortIds             Optional subset of cohortIds to generate. By default this function will sample all cohorts
#' @param seed                  Vector of seeds to give to the R psuedorandom number generator
#' @param seedArgs              optional arguments to pass to set.seed
#' @export
#' @returns                     sampledCohortDefinitionSet - a data.frame like object that contains the resulting identifiers and modified names of cohorts
#' @inheritParams               generateCohortSet
sampleCohortDefinitionSet <- function(cohortDefinitionSet,
                                      cohortIds = cohortDefinitionSet$cohortId,
                                      connectionDetails = NULL,
                                      connection = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      cohortDatabaseSchema,
                                      outputDatabaseSchema = cohortDatabaseSchema,
                                      cohortTableNames = getCohortTableNames(),
                                      n,
                                      seed = 64374,
                                      seedArgs = NULL,
                                      identifierExpression = "cohortId * 1000 + seed",
                                      incremental = FALSE,
                                      incrementalFolder = NULL) {

  checkmate::assertIntegerish(n, min.len = 1)
  checkmate::assertIntegerish(seed, min.len = 1)
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

    recordKeepingFile <- file.path(incrementalFolder, "GeneratedCohortSamples.csv")
  }

  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  .checkCohortTables(connection, cohortDatabaseSchema, cohortTableNames)
  sampledCohorts <-
    purrr::map2(seed, cohortIds, function(seed, targetCohortId) {
      sampledCohortDefinition <- cohortDefinitionSet %>%
        dplyr::filter(.data$cohortId == targetCohortId)

      sampledCohortDefinition$isSample <- TRUE
      outputCohortId <-  .computeIdentifierExpression(identifierExpression,
                                                      sampledCohortDefinition$cohortId,
                                                      seed)
      sampledCohortDefinition$sampleTargetCohortId <- sampledCohortDefinition$cohortId
      sampledCohortDefinition$cohortId <- outputCohortId
      sampledCohortDefinition$cohortName <- sprintf("%s [SAMPLE seed=%s n=%s]",
                                                    sampledCohortDefinition$cohortName, seed, n)
      if (incremental && !isTaskRequired(
        cohortId = targetCohortId,
        seed = seed,
        checksum = computeChecksum(paste0(cohortToSample$sql, n, seed)),
        recordKeepingFile = recordKeepingFile
      )) {
        return(sampledCohortDefinition)
      }
      # check incremental task for cohort sampling
      sampleTable <- .getSampleSet(connection = connection,
                                   n = n,
                                   seed = seed + targetCohortId, # Seed is unique to each target cohort
                                   seedArgs = seedArgs,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   targetCohortId = targetCohortId,
                                   targetTable = cohortTableNames$cohortTable)

      if (nrow(sampleTable == 0)) {
        ParallelLogger::logInfo("No entires found for ", targetCohortId, " was it generated?")
        return(sampledCohortDefinition)
      }
      # Called only for side effects
      .sampleCohort(connection = connection,
                    targetCohortId = targetCohortId,
                    targetTable = cohortTableNames$cohortTable,
                    outputCohortId = outputCohortId,
                    outputTable = cohortTableNames$cohortSampleTable,
                    cohortDatabaseSchema = cohortDatabaseSchema,
                    outputDatabaseSchema = outputDatabaseSchema,
                    sampleTable = sampleTable,
                    seed = seed + targetCohortId, # Seed is unique to each target cohort
                    tempEmulationSchema = tempEmulationSchema)

      return(sampledCohortDefinition)
    }) %>%
      dplyr::bind_rows()

  attr(sampledCohorts, "isSampledCohortDefinition") <- TRUE

  # deep clone any subset definitions
  if (hasSubsetDefinitions(cohortDefinitionSet)) {
    subsetDefintiions <- list()
    purrr::map(attr(cohortDefinitionSet, "cohortSubsetDefinitions"), function(subsetDefinition) {
      subsetDefintiions[[length(subsetDefintiions) + 1]] <- subsetDefinition$clone(deep = TRUE)
    })
    attr(sampledCohorts, "cohortSubsetDefinitions") <- subsetDefintiions
  }

  delta <- Sys.time() - start
  writeLines(paste("Generating sample set took", round(delta, 2), attr(delta, "units")))
  return(sampledCohorts)
}