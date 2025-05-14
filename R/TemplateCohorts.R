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

#' Class for automating the creation of bulk cohorts
#'
#' This class provides a framework for automating the creation of bulk cohorts
#' by defining template SQL queries and associated callbacks to execute them.
#' This is useful when defining lots of exposure or outcomes for cohorts that are very general in nature.
#' For example, all rxNorm ingredient cohorts, all ATC ingredient cohorts or all SNOMED condition occurences with > x
#' diagnosis codes.
#'
#' These cohorts can then be subsetted with common cohort subset operations such as limiting to specific age, gender,
#' or observation criteria, should this be excluded from the cohort definition. However, when applying operations in
#' bulk it may be more efficient to include such definitions within the template sql itself.
#'
#' This approach is also useful for cohorts that can not based on ATLAS/CirceDefinitions alone.
#'
#' CURRENTLY NOT SUPPORTED:
#'  * Saving definitions that use runtime arguments on a per cdm basis. This creates a challenge for running the same
#'    cohort across different databases. Furthermore, saving information within the CDM schema in a shared OHDSI study
#'    is not desirable.
#' @export
#' @family templateCohorts
CohortTemplateDefinition <- R6::R6Class(
  "CohortTemplateDefinition",
  private = list(
    .id = NULL,
    .checksum = NULL,
    .name = NULL,
    .references = NULL,
    .templateSql = NULL,
    .translateSql = NULL,
    .sqlArgs = NULL,
    generateId = function() {
      private$.checksum <- digest::digest(list(private$.references, private$.templateSql, private$.sqlArgs))
      private$.id <- paste0("CohortTemplate_", private$.checksum)
    },

    # Insert start time for template cohort
    insertStartTimeChecksum = function(connection,
                                       cohortDatabaseSchema,
                                       cohortTableNames) {
      startSql <- "DELETE FROM @results_database_schema.@cohort_checksum_table
                      WHERE cohort_definition_id IN (@target_cohort_ids) AND checksum = '@checksum';"
      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   startSql,
                                                   results_database_schema = cohortDatabaseSchema,
                                                   cohort_checksum_table = cohortTableNames$cohortChecksumTable,
                                                   target_cohort_ids = private$.references$cohortId,
                                                   checksum = self$getChecksum(),
                                                   reportOverallTime = FALSE,
                                                   progressBar = FALSE)
      batchSize <- 1000
      startTime <- as.numeric(Sys.time()) * 1000
      # NOTE: Batch insert ids - fails with bigints on spark - crossplatform workaround
      # Modifying DatabaseConnector here is not striaghtforward, this code largely copies its functionality
      for (start in seq(1, nrow(private$.references), by = batchSize)) {
        end <- min(start + batchSize - 1, nrow(private$.references))
        batch <- private$.references[start:end, , drop = FALSE]
        valuesString <- paste0(batch$cohortId, ", '", self$getChecksum(), "', ", startTime) |>
          paste(collapse = "),\n(")

        valuesString <- paste0("(", valuesString, ")")
        sql <- "INSERT INTO @results_database_schema.@cohort_checksum_table
                   (cohort_definition_id, checksum, start_time)
            VALUES @values"
        sql <- SqlRender::render(sql = sql,
                                 results_database_schema = cohortDatabaseSchema,
                                 cohort_checksum_table = cohortTableNames$cohortChecksumTable,
                                 values = valuesString)
        sql <- SqlRender::translate(sql, targetDialect = dbms(connection))
        DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

        # Delete existing batch ids from cohort table prior to generation as user may not have done this
        delSql <- "DELETE FROM @results_database_schema.@cohort_table WHERE cohort_definition_id IN (@cohort_ids)"
        DatabaseConnector::renderTranslateExecuteSql(connection,
                                                     delSql,
                                                     cohort_ids = batch$cohortId,
                                                     results_database_schema = cohortDatabaseSchema,
                                                     cohort_table = cohortTableNames$cohortTable,
                                                     progressBar = FALSE,
                                                     reportOverallTime = FALSE)
      }
      return(startTime)
    },

    # inserts timestamp for cohort ids/checksum post generation
    insertEndTimeChecksum = function(connection, cohortDatabaseSchema, cohortTableNames) {
      endTime <- as.numeric(Sys.time()) * 1000
      endSql <- "
        UPDATE @results_database_schema.@cohort_checksum_table
        SET end_time = @end_time
        WHERE cohort_definition_id IN (@target_cohort_ids)
        AND checksum = '@checksum';"

      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   endSql,
                                                   target_cohort_ids = private$.references$cohortId,
                                                   results_database_schema = cohortDatabaseSchema,
                                                   cohort_checksum_table = cohortTableNames$cohortChecksumTable,
                                                   checksum = self$getChecksum(),
                                                   end_time = endTime,
                                                   progressBar = FALSE,
                                                   reportOverallTime = FALSE)

      return(endTime)
    }
  ),
  active = list(
    #' @field name                name for this template definition that describes the cohorts it creation
    #' @field sqlArgs             optional arguments for sql
    #' @field templateSql         sql template
    #' @field translateSql        translate the sql for different platforms
    #' @field references          data.frame of name/id references for cohort template that aligns with cohort set
    name = function(val) {
      if (missing(val))
        return(private$.name)
      checkmate::assertString(val)
      private$.name <- val
    },

    sqlArgs = function(sqlArgs) {
      if (missing(sqlArgs))
        return(sqlArgs)
      # Check if eecuteFun are functions
      checkmate::assertList(sqlArgs)

      warnFields <- c("cohort_database_schema", "cdm_database_schema", "vocabulary_database_schema", "cohort_table")
      if (any(warnFields %in% names(sqlArgs)))
        warning(paste("Fields", paste(warnFields, collapse = ", "), "should not be included in template definitions"))

      private$.sqlArgs <- sqlArgs
      private$generateId()
    },

    templateSql = function(templateSql) {
      if (missing(templateSql))
        return(private$.templateSql)
      checkmate::assertString(templateSql)
      private$.templateSql <- templateSql
      private$generateId()
    },

    references = function(references) {
      if (missing(references))
        return(private$.references)

      checkmate::assertDataFrame(references, min.rows = 1)
      checkmate::assertNames(colnames(references), must.include = c("cohortId", "cohortName"))
      if (!"json" %in% colnames(references)) {
        references$json <- paste("{}")
      }

      # Cohort ID in sql for unqiueness in checksum
      if (is.null(references$sql))
        references$sql <- paste0("SELECT '", references$cohortId, " - ", self$getName(), "';")

      references$isTemplatedCohort <- TRUE

      private$.references <- references
      private$generateId()
    },

    translateSql = function(translateSql) {
      if (missing(translateSql))
        return(private$.translateSql)

      checkmate::assertLogical(translateSql)
      private$.translateSql <- translateSql
    }
  ),
  public = list(
    #' @param settings          Settings of object to load seealso createCohortTemplateDefinition
    initialize = function(settings) {
      self$translateSql <- settings$translateSql
      self$templateSql <- settings$templateSql
      self$name <- settings$name
      self$sqlArgs <- settings$sqlArgs
      self$references <- settings$references
    },

    #' To alter the execution, override this function in a subclass.
    #' This translates and executes the sql of the cohort definition
    #' Note that calling this function will generate the cohorts but will not do so in an incremental manner.
    #' Checksums and timestamps will, however, be added to the generation table
    #' ever want to call this function outside of a testing environment.
    #' It is best practice to always use the standard runCohortGeneration/generateCohortSet pipeline to ensure
    #' validity of execution steps.
    #'
    #' @template Connection
    #' @template CohortDatabaseSchema
    #' @template CdmDatabaseSchema
    #' @template CohortTableNames
    #' @param vocabularyDatabaseSchema      vocabulary database schema
    #' @param tempEmulationSchema           cdm temp emulation schema
    executeTemplateSql = function(connection,
                                  cohortDatabaseSchema,
                                  cdmDatabaseSchema,
                                  cohortTableNames,
                                  vocabularyDatabaseSchema = cdmDatabaseSchema,
                                  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
      checkmate::assertTRUE(DatabaseConnector::dbIsValid(connection))
      checkmate::assertString(cohortDatabaseSchema)
      checkmate::assertString(cdmDatabaseSchema)
      checkmate::assertString(vocabularyDatabaseSchema)
      checkmate::assertString(tempEmulationSchema, null.ok = TRUE)
      checkmate::assertList(cohortTableNames)
      checkmate::assertNames(names(cohortTableNames),
                             must.include = c("cohortTable", "cohortChecksumTable"))
      args <- private$.sqlArgs
      args$sql <- self$templateSql
      args$cohort_database_schema <- cohortDatabaseSchema
      args$cdm_database_schema <- cdmDatabaseSchema
      # This will throw warnings in a log of cases
      args$vocabulary_database_schema <- vocabularyDatabaseSchema
      args$cohort_table <- cohortTableNames$cohortTable
      sql <- do.call(SqlRender::render, args)
      if (self$translateSql) {
        args$tempEmulationSchema <- tempEmulationSchema
        sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection))
      }

      startTime <- private$insertStartTimeChecksum(connection, cohortDatabaseSchema, cohortTableNames)
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
      endTime <- private$insertEndTimeChecksum(connection, cohortDatabaseSchema, cohortTableNames)

      status <- list(
        generationStatus = "COMPLETE",
        # Date time conversion back to lubridate for compatibility
        startTime = lubridate::as_datetime(startTime / 1000),
        endTime = lubridate::as_datetime(endTime / 1000)
      )
      return(status)
    },
    #' get template references data.frame
    #' @description
    #' Returns data.frame of references
    getTemplateReferences = function() {
      return(self$references)
    },

    #' get the name of the definition
    #' @description
    #' Name field
    getName = function() {
      return(self$name)
    },

    #' get the generated id of the template definition
    #' @description
    #' this is not the cohort ids and is based off of the checksum of the template definition
    getId = function() {
      return(private$.id)
    },

    #' get checksum
    #' @description
    #' Get the hash of the definition (generated when class is instantiated)
    getChecksum = function() {
      return(private$.checksum)
    },

    #' to list
    #' @description
    #' For seralizing the definition
    toList = function() {
      def <- list(
        name = self$name,
        references = self$references,
        templateSql = self$templateSql,
        sqlArgs = self$sqlArgs,
        translateSql = self$translateSql
      )
      return(def)
    },

    #' to json
    #' @description
    #' json seraalized form of the template definition
    toJson = function() {
      .toJSON(self$toList())
    },

    #' save to disk
    #' @description
    #' Save object to specified json path
    #' @param filePath      File path to save json serialized from
    saveTemplate = function(filePath) {
      checkmate::assertPathForOutput(filePath, overwrite = TRUE)
      ParallelLogger::saveSettingsToJson(filePath, self$toList())
    }
  )
)

#' Create Cohort Template Definition
#' @description construct a cohort template definition
#' @param name                    A name for the template definition. This is not used in the checksum of the cohort
#'
#' @param sqlArgs                 Optional parameters for execution of the query - for example vocabulary schema
#'                                These are arguments that should be passed to the sql. These are used in the checksum
#'                                if using paramtaried sql for different definitions (e.g. a definition requiring
#'                                varying observation lengths. This is used to distingish them)
#'                                This should not include cdm/data source
#'                                specfic parameters such as the cohort table names,
#'                                cdm database schema or vocabulary database schema. If the definition requires
#'                                runtime specific arguments (e.g. non standard tables) this presents a problem
#'                                for seralizing and uniqiuely idenitifying template cohort definitions.
#' @param references              This is a data frame that must contain cohortId and cohortName. Optionally, this
#'                                can contain the columns sql and json as well. It must be bindable to a
#'                                cohort definition set instance.
#' @param templateSql             Sql string that is used to generate the cohorts. This should be in OHDSI sql
#'                                form, translatable to other db platforms.
#' @param translateSql            to translate the sql or not.
#' @export
#' @family templateCohorts
createCohortTemplateDefintion <- function(name,
                                          templateSql,
                                          references,
                                          sqlArgs = list(),
                                          translateSql = TRUE) {
  settings <- list(name = name,
                   sqlArgs = sqlArgs,
                   references = references,
                   templateSql = templateSql,
                   translateSql = translateSql)

  def <- CohortTemplateDefinition$new(settings)
  return(invisible(def))
}

#' Extract template definitions from a cohort definition set
#' @template cohortDefinitionSet
#' @family templateCohorts
getTemplateDefinitions <- function(cohortDefinitionSet) {
  checkmate::assertDataFrame(cohortDefinitionSet, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "sql"
                         )
  )

  templates <- attr(cohortDefinitionSet, "templateCohortDefinitions")
  if (is.null(templates)) {
    templates <- list()
  }
  return(templates)
}


#' Add Cohort template definition to cohort set
#' @description Adds a cohort template definition to an existing cohort definition set or creates one if none provided
#' @inheritParams generateCohortSet
#' @export
#' @param references                A data.frame containing all cohort ids and names for cohorts. Cohort Id must be
#'                                  unique and not clash with any cohorts in the existing cohort definition set.
#'                                  Json can optionally be provided. For example, in the case where the sql is intended
#'                                  as a bulk speed up, it is likely possible to have an equivalent Circe cohort
#'                                  definition.
#' @param cohortTemplateDefintion   An instance of CohortTemplateDefinition (or subclass)
#' @family templateCohorts
addCohortTemplateDefintion <- function(cohortDefinitionSet = createEmptyCohortDefinitionSet(),
                                       cohortTemplateDefintion) {
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertR6(cohortTemplateDefintion, "CohortTemplateDefinition")

  if (is.null(attr(cohortDefinitionSet, "templateCohortDefinitions"))) {
    attr(cohortDefinitionSet, "templateCohortDefinitions") <- list()

    if (nrow(cohortDefinitionSet) > 0)
      cohortDefinitionSet$isTemplatedCohort <- FALSE
  }
  tplId <- cohortTemplateDefintion$getId()
  templateDefs <- attr(cohortDefinitionSet, "templateCohortDefinitions")
  if (tplId %in% names(templateDefs)) {
    stop("Template definition with the same ID already added to cohort definition set")
  }

  references <- cohortTemplateDefintion$getTemplateReferences()
  # Assert ids are not in
  if (any(references$cohortId %in% cohortDefinitionSet$cohortId)) {
    stop("Cannot add reference set to cohort as it would result in non-unique cohort identifiers")
  }

  templateDefs[[tplId]] <- cohortTemplateDefintion
  attr(cohortDefinitionSet, "templateCohortDefinitions") <- templateDefs
  cohortDefinitionSet <- dplyr::bind_rows(cohortDefinitionSet, references)

  return(cohortDefinitionSet)
}

#' Add an sql cohort definition
#' @description
#' This is useful in cases where it is difficult or impossible to define a cohort in Circe.
#' This utility should be used sparingly, but is convenient non-the-less.
#' Note that no checks on this definition occur and, in principle, any sql can be executed.
#' Incremental execution and logging will work. This should also be compatible with other OHDSI packages that use
#' standard cohort tables.
#'
#' All cohorts should result in standard cohort tables which have the columns:
#'
#' * cohort_definition_id,
#' * subject_id,
#' * cohort_start_date,
#' * cohort_end_date
#'
#' As these are requirements of cohorts.
#'
#' The sql parameters:
#' cohort_table, cohort_database_schema, cdm_database_schema and vocabulary_database_schema should not be specified
#' in the arguments to this function. These cohorts can be serialized with saveCohortDefinitionSet and shared so should
#' not include data source specific content.
#'
#'
#'
#' @template cohortDefinitionSet
#' @param cohortId        Id of cohort to add. Must be unique in the cohort definition set
#' @param cohortName      Name of the cohort to add
#' @param sql             Ohdsi Stanaard sql
#' @param json            optional json parameters
#' @param ...             arguments for the sql. Note that this does not need to include cohort_table,
#'                        cohort_database_schema, cdm_database_schema or vocabulary_database_schema
#' @param tanslateSql     perfom translation on the sql. This is ignored if the sql has already been translated
#'                        with the sql render function.
#' @family templateCohorts
#' @export
#' @examples
#'
#' sql <- "INSERT INTO @cohort_database_schema.@cohort_table
#'              (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
#'         SELECT 1 as cohort_definition_id,
#'                person_id as subject_id,
#'                drug_era_start_date as cohort_start_date,
#'                drug_era_end_data as cohort_end_date
#'         FROM @cdm_database_schema.drug_era de
#'         INNER JOIN @vocabulary_database_schema.concept c on de.drug_concept_id = c.concept_id
#'         -- Find any matches of drugs named 'asprin' in the drug concept table
#'         WHERE lower(c.concept_name) like '%asprin%'; "
#'
#' cohortDefinitionSet = createEmptyCohortDefinitionSet() |>
#'  addSqlCohortDefinition(sql = sql, cohortId = 1, cohortName = "my asprin cohort")
#'
#'
addSqlCohortDefinition <- function(cohortDefinitionSet, sql, cohortId, cohortName, tanslateSql = TRUE, json = NULL, ...) {
  checkmate::assertString(sql)
  checkmate::assertString(cohortName)
  checkmate::assertNumeric(cohortId, len = 1)
  tplDef <- createCohortTemplateDefintion(name = cohortName,
                                          templateSql = sql,
                                          references = data.frame(cohortId = cohortId,
                                                                  cohortName = cohortName,
                                                                  sql = sql),
                                          sqlArgs = list(...),
                                          translateSql = tanslateSql)

  cohortDefinitionSet <- addCohortTemplateDefintion(cohortDefinitionSet, tplDef)
  return(cohortDefinitionSet)
}

#' Function to iterate over template cohorts and incrementally generate
#' Called by generateCohortSet
#' @noRd
generateTemplateCohorts <- function(connection,
                                    cohortDefinitionSet,
                                    cdmDatabaseSchema,
                                    tempEmulationSchema,
                                    cohortDatabaseSchema,
                                    cohortTableNames,
                                    stopOnError,
                                    incremental,
                                    recordKeepingFile) {

  templateDefs <- getTemplateDefinitions(cohortDefinitionSet)
  statusTbl <- data.frame()
  computedChecksums <- getLastGeneratedCohortChecksums(connection = connection,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       cohortTableNames = cohortTableNames)

  for (template in templateDefs) {
    startTime <- lubridate::now()
    refs <- template$getTemplateReferences()
    skipit <- all(incremental,
                  refs$cohortId %in% computedChecksums$cohortDefinitionId,
                  template$getChecksum() %in% computedChecksums$checksum)
    if (skipit) {
      status <- list(startTime = startTime, endTime = startTime, generationStatus = "SKIPPED")
    } else {
      status <- tryCatch({
        ParallelLogger::logInfo("Generating Template Cohort: ", template$getName())
        status <- template$executeTemplateSql(connection = connection,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              tempEmulationSchema = tempEmulationSchema,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              cohortTableNames = cohortTableNames)
        ParallelLogger::logInfo("Template Cohort complete: ", template$getName())
        # legacy task log not inside class as this will probably be removed
        if (incremental) {
          recordTasksDone(
            cohortId = refs$cohortId,
            checksum = template$getChecksum(),
            recordKeepingFile = recordKeepingFile
          )
        }
        status
      }, error = function(err) {
        ParallelLogger::logError(err)
        if (stopOnError)
          stop(err)
        return(list(startTime = startTime, endTime = lubridate::now(), generationStatus = "FAILED"))
      })
    }
    statusTbl <- statusTbl |> dplyr::bind_rows(
      data.frame(cohortId = refs$cohortId,
                 cohortName = refs$cohortName,
                 generationStatus = status$generationStatus,
                 startTime = status$startTime,
                 endTime = status$endTime,
                 checksum = template$getChecksum())
    )
  }

  return(statusTbl)
}

loadTemplateFromJson <- function(filePath) {
  CohortTemplateDefinition$new(ParallelLogger::loadSettingsFromJson(filePath))
}


saveCohortTemplateDefinitions <- function(templateDefinitions, templateFolder) {
  rlang::inform("saving cohort template definions...")
  dir.create(templateFolder, recursive = TRUE, showWarnings = FALSE)
  tplOrder <- 1
  lapply(templateDefinitions, function(tpl) {
    tpl$saveTemplate(file.path(templateFolder, paste0(tplOrder, ".json")))
    tplOrder <<- tplOrder + 1
  })
  rlang::inform("saved cohort template definions")
}


loadTemplateDefinitionsFolder <- function(cohortDefinitionSet, templateFolder) {
  templateDefinitions <- list()
  files <- list.files(path = templateFolder, pattern = "^[0-9]+\\.json$", full.names = TRUE)
  # Order numerically
  files <- files[order(as.numeric(gsub("\\.json$", "", basename(files))))]
  for (jsonFile in files) {
    template <- loadTemplateFromJson(jsonFile)
    cohortDefinitionSet <- cohortDefinitionSet |>
      addCohortTemplateDefintion(template)
  }

  return(cohortDefinitionSet)
}
