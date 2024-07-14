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

#' Run a cohort generation and export results
#'
#' @details
#' Run a cohort generation for a set of cohorts and negative control outcomes.
#' This function will also export the results of the run to the `outputFolder`.
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTableNames
#'
#' @template CohortDefinitionSet
#'
#' @template NegativeControlOutcomeCohortSet
#'
#' @param occurrenceType     For negative controls outcomes, the occurrenceType
#'                           will detect either: the first time an
#'                           outcomeConceptId occurs or all times the
#'                           outcomeConceptId occurs for a person. Values
#'                           accepted: 'all' or 'first'.
#'
#' @param detectOnDescendants For negative controls outcomes, when set to TRUE,
#'                            detectOnDescendants will use the vocabulary to
#'                            find negative control outcomes using the
#'                            outcomeConceptId and all descendants via the
#'                            concept_ancestor table. When FALSE, only the exact
#'                            outcomeConceptId will be used to detect the
#'                            outcome.
#'
#' @param stopOnError         If an error happens while generating one of the
#'                            cohorts in the cohortDefinitionSet, should we
#'                            stop processing the other cohorts? The default is
#'                            TRUE; when set to FALSE, failures will be
#'                            identified in the return value from this function.
#'
#' @param outputFolder Name of the folder where all the outputs will written to.
#'
#' @param databaseId    A unique ID for the database. This will be appended to
#'                      most tables.
#'
#' @param incremental   Create only cohorts that haven't been created before?
#'
#' @param incrementalFolder If \code{incremental = TRUE}, specify a folder where
#'                          records are kept of which definition has been
#'                          executed.
#'
#' @export
runCohortGeneration <- function(connectionDetails,
                                cdmDatabaseSchema,
                                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                cohortDatabaseSchema = cdmDatabaseSchema,
                                cohortTableNames = getCohortTableNames(),
                                cohortDefinitionSet = NULL,
                                negativeControlOutcomeCohortSet = NULL,
                                occurrenceType = "all",
                                detectOnDescendants = FALSE,
                                stopOnError = TRUE,
                                outputFolder,
                                databaseId = 1,
                                incremental = FALSE,
                                incrementalFolder = NULL) {
  if (is.null(cohortDefinitionSet) && is.null(negativeControlOutcomeCohortSet)) {
    stop("You must supply at least 1 cohortDefinitionSet OR 1 negativeControlOutcomeCohortSet")
  }
  errorMessages <- checkmate::makeAssertCollection()
  if (is(connectionDetails, "connectionDetails")) {
    checkmate::assertClass(connectionDetails, "connectionDetails", add = errorMessages)
  } else {
    checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  }
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertList(cohortTableNames, min.len = 1, add = errorMessages)
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertDataFrame(negativeControlOutcomeCohortSet, min.rows = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assert_choice(x = tolower(occurrenceType), choices = c("all", "first"), add = errorMessages)
  checkmate::assert_logical(detectOnDescendants, add = errorMessages)
  checkmate::assert_logical(stopOnError, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  # Establish the connection and ensure the cleanup is performed
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # Create the export folder
  if (!dir.exists(outputFolder)) {
    dir.create(outputFolder, recursive = T)
  }

  # Create the cohort tables
  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    incremental = incremental
  )

  generateAndExportCohorts(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    stopOnError = stopOnError,
    outputFolder = outputFolder,
    databaseId = databaseId,
    incremental = incremental,
    incrementalFolder = incrementalFolder
  )

  generateAndExportNegativeControls(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
    occurrenceType = occurrenceType,
    detectOnDescendants = detectOnDescendants,
    outputFolder = outputFolder,
    databaseId = databaseId,
    incremental = incremental,
    incrementalFolder = incrementalFolder
  )

  # Export the results data model specification
  file.copy(
    from = system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator"),
    to = outputFolder
  )

  rlang::inform("Cohort generation complete.")
}

generateAndExportCohorts <- function(connection,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema,
                                     cohortDatabaseSchema,
                                     cohortTableNames,
                                     cohortDefinitionSet,
                                     stopOnError,
                                     outputFolder,
                                     databaseId,
                                     incremental,
                                     incrementalFolder) {
  # Generate the cohorts
  cohortsGenerated <- createEmptyResult("cg_cohort_generation")
  cohortsGeneratedFileName <- file.path(outputFolder, "cg_cohort_generation.csv")
  cohortCounts <- createEmptyResult("cg_cohort_count")
  cohortCountsFileName <- file.path(outputFolder, "cg_cohort_count.csv")
  if (!is.null(cohortDefinitionSet)) {
    # Generate cohorts, get counts, write results
    cohortsGenerated <- generateCohortSet(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      stopOnError = stopOnError,
      incremental = incremental,
      incrementalFolder = incrementalFolder
    )

    cohortCountsFromDb <- getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      databaseId = databaseId
    )

    # Filter to columns in the results data model
    cohortCounts <- cohortCountsFromDb[names(cohortCounts)]
  }

  # Save the generation information
  rlang::inform("Saving cohort generation information")
  if (!is.null(cohortsGenerated) && nrow(cohortsGenerated) > 0) {
    cohortsGenerated$databaseId <- databaseId
    # Remove any cohorts that were skipped
    cohortsGenerated <- cohortsGenerated[toupper(cohortsGenerated$generationStatus) != "SKIPPED", ]
    if (incremental) {
      # Format the data for saving
      names(cohortsGenerated) <- SqlRender::camelCaseToSnakeCase(names(cohortsGenerated))
      saveIncremental(
        data = cohortsGenerated,
        fileName = cohortsGeneratedFileName,
        cohort_id = cohortsGenerated$cohort_id
      )
    } else {
      writeCsv(
        x = cohortsGenerated,
        file = cohortsGeneratedFileName
      )
    }
  }

  rlang::inform("Saving cohort counts")
  writeCsv(
    x = cohortCounts,
    file = cohortCountsFileName
  )

  rlang::inform("Saving cohort statistics")
  exportCohortStatsTables(
    connection = connection,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortStatisticsFolder = outputFolder,
    snakeCaseToCamelCase = FALSE,
    fileNamesInSnakeCase = TRUE,
    incremental = incremental,
    databaseId = databaseId,
    cohortDefinitionSet = cohortDefinitionSet,
    tablePrefix = "cg_"
  )

  # Export the cohort definition set
  rlang::inform("Saving cohort definition set")
  exportCohortDefinitionSet(outputFolder, cohortDefinitionSet)
}

generateAndExportNegativeControls <- function(connection,
                                              cdmDatabaseSchema,
                                              tempEmulationSchema,
                                              cohortDatabaseSchema,
                                              cohortTableNames,
                                              negativeControlOutcomeCohortSet,
                                              occurrenceType,
                                              detectOnDescendants,
                                              outputFolder,
                                              databaseId,
                                              incremental,
                                              incrementalFolder) {
  # Generate any negative controls
  negativeControlOutcomes <- createEmptyResult("cg_cohort_definition_neg_ctrl")
  negativeControlOutcomesFileName <- file.path(outputFolder, "cg_cohort_definition_neg_ctrl.csv")
  cohortCountsNegativeControlOutcomes <- createEmptyResult("cg_cohort_count_neg_ctrl")
  cohortCountsNegativeControlOutcomesFileName <- file.path(outputFolder, "cg_cohort_count_neg_ctrl.csv")
  if (!is.null(negativeControlOutcomeCohortSet)) {
    generateNegativeControlOutcomeCohorts(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      negativeControlOutcomeCohortSet = negativeControlOutcomeCohortSet,
      tempEmulationSchema = tempEmulationSchema,
      occurrenceType = occurrenceType,
      detectOnDescendants = detectOnDescendants,
      incremental = incremental,
      incrementalFolder = incrementalFolder
    )

    # Assemble the negativeControlOutcomes for export
    negativeControlOutcomes <- cbind(
      negativeControlOutcomeCohortSet,
      occurrenceType = rep(occurrenceType, nrow(negativeControlOutcomeCohortSet)),
      detectOnDescendants = rep(detectOnDescendants, nrow(negativeControlOutcomeCohortSet))
    )

    # Count the negative controls
    cohortCountsNegativeControlOutcomes <- getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      databaseId = databaseId,
      cohortDefinitionSet = negativeControlOutcomeCohortSet[, c("cohortId"), drop = FALSE]
    )
  }

  rlang::inform("Saving negative control outcome cohort definition")
  writeCsv(
    x = negativeControlOutcomes,
    file = negativeControlOutcomesFileName
  )

  rlang::inform("Saving negative control outcome cohort counts")
  writeCsv(
    x = cohortCountsNegativeControlOutcomes,
    file = cohortCountsNegativeControlOutcomesFileName
  )
}
