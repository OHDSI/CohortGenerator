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

.defaultNameTemplate <- "@baseCohortName - @subsetDefinitionName @operatorNames"

# CohortSubsetDefinition ------------------------------
#' @title Cohort Subset Definition
#' @export
#' @description
#' Set of subset definitions
CohortSubsetDefinition <- R6::R6Class(
  classname = "CohortSubsetDefinition",
  private = list(
    .name = "",
    .operatorNameConcatString = "",
    .subsetCohortNameTemplate = "",
    .definitionId = integer(0),
    .subsetOperators = list(),
    .targetOutputPairs = list(),
    .identifierExpression = expression(targetId * 1000 + definitionId),
    ## Creates objects if they are in the namespace
    createSubset = function(item, itemClass = item$subsetType) {
      classDef <- get(itemClass)
      checkmate::assertClass(classDef, "R6ClassGenerator")
      obj <- classDef$new(item)
      checkmate::assertR6(obj, "SubsetOperator")
      return(obj)
    }
  ),
  public = list(
    #' @param definition  json or list representation of object
    initialize = function(definition = NULL) {
      if (!is.null(definition)) {
        definition <- .loadJson(definition)
        self$name <- definition$name
        self$definitionId <- definition$definitionId
        self$targetOutputPairs <- definition$targetOutputPairs
        self$subsetOperators <- lapply(definition$subsetOperators, private$createSubset)
        self$operatorNameConcatString <- definition$operatorNameConcatString
        self$subsetCohortNameTemplate <- definition$subsetCohortNameTemplate
      }
      self
    },
    #' to List
    #' @description List representation of object
    toList = function() {
      list(
        name = jsonlite::unbox(self$name),
        definitionId = jsonlite::unbox(self$definitionId),
        # Note - when there is a base definition that includes multiple calls to the same subset this should be replaced
        subsetOperators = lapply(self$subsetOperators, function(operator) {
          operator$toList()
        }),
        packageVersion = jsonlite::unbox(as.character(utils::packageVersion(utils::packageName()))),
        identifierExpression = jsonlite::unbox(as.character(private$.identifierExpression)),
        operatorNameConcatString = jsonlite::unbox(as.character(private$.operatorNameConcatString)),
        subsetCohortNameTemplate = jsonlite::unbox(as.character(private$.subsetCohortNameTemplate))
      )
    },
    #' to JSON
    #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

    #' add Subset Operator
    #' @description add subset to class - checks if equivalent id is present
    #' Will throw an error if a matching ID is found but reference object is different
    #' @param subsetOperator a SubsetOperator instance
    #' @param overwrite if a subset operator of the same ID is present, replace it with a new definition
    addSubsetOperator = function(subsetOperator) {
      checkmate::assertR6(subsetOperator, "SubsetOperator")
      private$.subsetOperators <- c(private$.subsetOperators, subsetOperator$clone(deep = TRUE))
      self
    },

    #' get query for a given target output pair
    #' @description
    #' Returns vector of join, logic, having statements returned by subset operations
    #' @param targetOutputPair              Target output pair
    getSubsetQuery = function(targetOutputPair) {
      checkmate::assertIntegerish(targetOutputPair, len = 2)
      checkmate::assertFALSE(targetOutputPair[[1]] == targetOutputPair[[2]])

      targetTable <- "#cohort_sub_base"
      sql <- c(
        "DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @output_cohort_id;",
        "DROP TABLE IF EXISTS #cohort_sub_base;",
        "SELECT * INTO #cohort_sub_base FROM @cohort_database_schema.@cohort_table",
        "WHERE cohort_definition_id = @target_cohort_id;"
      )

      dropTables <- c(targetTable)
      for (i in 1:length(self$subsetOperators)) {
        subsetOperator <- self$subsetOperators[[i]]
        queryBuilder <- subsetOperator$getQueryBuilder(i)
        sql <- c(sql, queryBuilder$getQuery(targetTable))
        targetTable <- queryBuilder$getTableObjectId()
        dropTables <- c(dropTables, targetTable)
      }

      sql <- c(sql, SqlRender::readSql(system.file("sql", "sql_server", "subsets", "CohortSubsetDefinition.sql", package = "CohortGenerator")))
      # Cleanup after exectuion
      for (table in dropTables) {
        sql <- c(sql, SqlRender::render("DROP TABLE IF EXISTS @table;", table = table))
      }
      sql <- paste(sql, collapse = "\n")

      sql <- SqlRender::render(sql,
        output_cohort_id = targetOutputPair[2],
        target_cohort_id = targetOutputPair[1],
        target_table = targetTable,
        warnOnMissingParameters = FALSE
      )

      return(sql)
    },

    #' Get name of an output cohort
    #' @param cohortDefinitionSet           Cohort definition set containing base names
    #' @param targetOutputPair              Target output pair
    getSubsetCohortName = function(cohortDefinitionSet, targetOutputPair) {
      checkmate::assertIntegerish(targetOutputPair, len = 2)
      checkmate::assertFALSE(targetOutputPair[[1]] == targetOutputPair[[2]])
      checkmate::assertTRUE(targetOutputPair[[1]] %in% cohortDefinitionSet$cohortId)
      checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))

      baseName <- cohortDefinitionSet %>%
        dplyr::filter(cohortId == targetOutputPair[1]) %>%
        dplyr::select("cohortName") %>%
        dplyr::pull()

      opNameList <- lapply(self$subsetOperators, function(x) x$name)
      opNames <- paste0(opNameList, collapse = self$operatorNameConcatString)

      SqlRender::render(self$subsetCohortNameTemplate,
        baseCohortName = baseName,
        subsetDefinitionName = self$name,
        operatorNames = opNames,
        warnOnMissingParameters = FALSE
      )
    },
    #' Set the targetOutputPairs to be added to a cohort definition set
    #' @param targetIds   list of cohort ids to apply subsetting operations to
    setTargetOutputPairs = function(targetIds) {
      checkmate::assertIntegerish(targetIds, min.len = 1, upper = 10e11)
      definitionId <- self$definitionId
      targetOutputPairs <- list()

      for (targetId in targetIds) {
        outputId <- eval(self$identifierExpression)
        targetOutputPairs <- c(targetOutputPairs, list(c(targetId, outputId)))
      }
      self$targetOutputPairs <- targetOutputPairs

      invisible(self)
    },

    #' Get json file name for subset definition in folder
    #' @param subsetJsonFolder  path to folder to place file
    getJsonFileName = function(subsetJsonFolder = "inst/cohort_subset_definitions/") {
      return(file.path(subsetJsonFolder, paste0(self$definitionId, ".json")))
    }
  ),
  active = list(
    #' @field targetOutputPairs  list of pairs of integers - (targetCohortId, outputCohortId)
    targetOutputPairs = function(targetOutputPairs) {
      if (missing(targetOutputPairs)) {
        return(private$.targetOutputPairs)
      }

      if (is.null(targetOutputPairs)) {
        targetOutputPairs <- list()
      }
      checkmate::assertList(targetOutputPairs, types = c("numeric", "list"), unique = TRUE)
      targetOutputPairs <- lapply(
        targetOutputPairs,
        function(targetOutputPair) {
          targetOutputPair <- as.numeric(targetOutputPair)
          checkmate::assertIntegerish(targetOutputPair, len = 2, upper = 10e11)
          checkmate::assertFALSE(targetOutputPair[[1]] == targetOutputPair[[2]])
          targetOutputPair
        }
      )

      private$.targetOutputPairs <- targetOutputPairs
      self
    },
    #' @field subsetOperators list of subset operations
    subsetOperators = function(subsetOperators) {
      if (missing(subsetOperators)) {
        # We don't want to return references to the operators in case users modify them after this
        return(lapply(private$.subsetOperators, function(x) {
          x$clone(deep = TRUE)
        }))
      }

      checkmate::assertList(subsetOperators, types = "SubsetOperator")
      lapply(subsetOperators, self$addSubsetOperator)
      self
    },
    #' @field name name of definition
    name = function(name) {
      if (missing(name)) {
        return(private$.name)
      }

      checkmate::assertCharacter(name)
      private$.name <- name
      self
    },
    #' @field subsetCohortNameTemplate template string for formatting resulting cohort names
    subsetCohortNameTemplate = function(subsetCohortNameTemplate) {
      if (missing(subsetCohortNameTemplate)) {
        return(private$.subsetCohortNameTemplate)
      }

      if (is.null(subsetCohortNameTemplate)) {
        subsetCohortNameTemplate <- ""
      }

      checkmate::assertCharacter(subsetCohortNameTemplate)

      if (subsetCohortNameTemplate == "") {
        # Set to default subsetCohortNameTemplate
        subsetCohortNameTemplate <- .defaultNameTemplate
      }

      private$.subsetCohortNameTemplate <- subsetCohortNameTemplate
      self
    },

    #' @field operatorNameConcatString string used when concatenating operator names together
    operatorNameConcatString = function(operatorNameConcatString) {
      if (missing(operatorNameConcatString)) {
        return(private$.operatorNameConcatString)
      }

      if (is.null(operatorNameConcatString)) {
        operatorNameConcatString <- ""
      }

      if (operatorNameConcatString == "") {
        operatorNameConcatString <- ", "
      }
      checkmate::assertCharacter(operatorNameConcatString)

      private$.operatorNameConcatString <- operatorNameConcatString
      self
    },

    #' @field definitionId numeric definition id
    definitionId = function(definitionId) {
      if (missing(definitionId)) {
        return(private$.definitionId)
      }

      checkmate::assertInt(definitionId)
      private$.definitionId <- definitionId
      self
    },
    #' @field identifierExpression expression that can be evaluated from
    identifierExpression = function(identifierExpression) {
      if (missing(identifierExpression)) {
        return(private$.identifierExpression)
      }

      if (is.character(identifierExpression)) {
        identifierExpression <- parse(text = identifierExpression)
      }

      if (is.null(identifierExpression)) {
        identifierExpression <- expression(targetId * 1000 + definitionId)
      }

      checkmate::assertTRUE(is.expression(identifierExpression))

      private$.identifierExpression <- identifierExpression
      self
    }
  )
)

# createCohortSubsetDefinition ------------------------------
#' Create Subset Definition
#' @description
#' Create subset definition from subset objects
#' @export
#' @param name                      Name of definition
#' @param definitionId              Definition identifier
#' @param subsetOperators           list of subsetOperator instances to apply
#' @param identifierExpression      Expression (or string that converts to expression) that returns an id for an output cohort
#'                                  the default is dplyr::expr(targetId * 1000 + definitionId)
#' @param subsetCohortNameTemplate  (optional) SqlRender string template for formatting names of resulting subset cohorts
#'                                  Can use the variables @baseCohortName, @subsetDefinitionName and @operatorNames.
#'                                  This is applied when adding the subset definition to a cohort definition set.
#' @param operatorNameConcatString  (optional) String to concatenate operator names together when outputting resulting cohort
#'                                   name
createCohortSubsetDefinition <- function(name,
                                         definitionId,
                                         subsetOperators,
                                         identifierExpression = NULL,
                                         operatorNameConcatString = "",
                                         subsetCohortNameTemplate = "") {
  subsetDef <- CohortSubsetDefinition$new()
  subsetDef$name <- name
  subsetDef$definitionId <- definitionId
  subsetDef$subsetOperators <- subsetOperators
  subsetDef$identifierExpression <- identifierExpression
  subsetDef$operatorNameConcatString <- operatorNameConcatString
  subsetDef$subsetCohortNameTemplate <- subsetCohortNameTemplate
  return(subsetDef)
}


#' Add cohort subset definition to a cohort definition set
#' @description
#' Given a subset definition and cohort definition set, this function returns a modified cohortDefinitionSet
#' That contains cohorts that's have parent's contained within the base cohortDefinitionSet
#'
#' Also adds the columns subsetParent and isSubset that denote if the cohort is a subset and what the parent definition
#' is.
#' @export
#' @param cohortDefinitionSet       data.frame that conforms to CohortDefinitionSet
#' @param cohortSubsetDefintion     CohortSubsetDefinition instance
#' @param targetCohortIds           Cohort ids to apply subset definition to. If not set, subset definition is applied
#'                                  to all base cohorts in set (i.e. those that are not defined by subsetOperators).
#'                                  Applying to cohorts that are already subsets is permitted, however, this should be
#'                                  done with care and identifiers must be specified manually
#' @param overwriteExisting         Overwrite existing subset definition of the same definitionId if present
addCohortSubsetDefinition <- function(cohortDefinitionSet,
                                      cohortSubsetDefintion,
                                      targetCohortIds = NULL,
                                      overwriteExisting = FALSE) {
  checkmate::assertR6(cohortSubsetDefintion, "CohortSubsetDefinition")
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))

  if (!"subsetParent" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$subsetParent <- cohortDefinitionSet$cohortId
  }

  if (!"isSubset" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$isSubset <- FALSE
  }

  if (!is.null(targetCohortIds)) {
    checkmate::assertSubset(targetCohortIds, cohortDefinitionSet$cohortId)
  } else {
    targetCohortIds <- cohortDefinitionSet %>%
      dplyr::filter(!.data$isSubset) %>%
      dplyr::select("cohortId") %>%
      dplyr::pull()
  }

  if (is.null(attr(cohortDefinitionSet, "cohortSubsetDefinitions"))) {
    attr(cohortDefinitionSet, "cohortSubsetDefinitions") <- list()
  }
  existingSubsetDefinitions <- attr(cohortDefinitionSet, "cohortSubsetDefinitions")

  if (!"subsetDefinitionId" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$subsetDefinitionId <- NA
  }

  # store a copy for simplicity with reference errors
  subsetDefinitionCopy <- cohortSubsetDefintion$clone(deep = TRUE)

  # Remove any cohorts that use this id
  findSubsetIndexById <- function(existingSubsetDefinitions, id) {
    if (length(existingSubsetDefinitions)) {
      for (i in 1:length(existingSubsetDefinitions)) {
        if (existingSubsetDefinitions[[i]]$definitionId == id) {
          return(i)
        }
      }
    }
    return(NA)
  }

  subsetIndex <- findSubsetIndexById(existingSubsetDefinitions, subsetDefinitionCopy$definitionId)
  if (!is.na(subsetIndex)) {
    if (overwriteExisting) {
      # Remove any cohorts that were created with this definition
      cohortDefinitionSet <- cohortDefinitionSet %>%
        dplyr::filter(is.na(.data$subsetDefinitionId) | .data$subsetDefinitionId != subsetDefinitionCopy$definitionId)
    } else {
      stop(
        "Existing definition of id ", subsetDefinitionCopy$definitionId,
        " already applied to set, use overwriteExisting = TRUE to re-apply or change definition id"
      )
    }
  } else {
    subsetIndex <- length(existingSubsetDefinitions) + 1
  }

  existingSubsetDefinitions[[subsetIndex]] <- subsetDefinitionCopy
  attr(cohortDefinitionSet, "cohortSubsetDefinitions") <- existingSubsetDefinitions

  subsetDefinitionCopy$setTargetOutputPairs(targetCohortIds)
  for (toPair in subsetDefinitionCopy$targetOutputPairs) {
    if (!toPair[1] %in% cohortDefinitionSet$cohortId) {
      stop("Target cohortid ", toPair[1], " not found in cohort definition set")
    }

    if (toPair[2] %in% cohortDefinitionSet$cohortId) {
      stop("Output cohort id ", toPair[2], " found in cohort definition set - must be a unique indentifier")
    }

    subsetSql <- cohortSubsetDefintion$getSubsetQuery(toPair)
    subsetCohortName <- cohortSubsetDefintion$getSubsetCohortName(cohortDefinitionSet, toPair)
    repr <- list(
      cohortId = toPair[2],
      targetCohortId = toPair[1],
      subsetDefinitionId = subsetDefinitionCopy$definitionId
    )
    cohortDefinitionSet <-
      dplyr::bind_rows(
        cohortDefinitionSet,
        data.frame(
          cohortId = toPair[2],
          cohortName = subsetCohortName,
          subsetParent = toPair[1],
          isSubset = TRUE,
          sql = subsetSql,
          json = as.character(.toJSON(repr)),
          subsetDefinitionId = subsetDefinitionCopy$definitionId
        )
      )
  }

  attr(cohortDefinitionSet, "hasSubsetDefinitions") <- TRUE

  return(cohortDefinitionSet)
}

hasSubsetDefinitions <- function(x) {
  containsSubsetsDefs <- length(attr(x, "cohortSubsetDefinitions")) > 0

  if (!containsSubsetsDefs) {
    warns <- checkmate::checkList(attr(x, "cohortSubsetDefinitions"),
      min.len = 1,
      types = "CohortSubsetDefinition"
    )
    if (length(warns)) {
      containsSubsetsDefs <- FALSE
    }
  }

  hasColumns <- all(c("subsetDefinitionId", "isSubset", "subsetParent") %in% colnames(x))

  return(all(
    hasColumns,
    containsSubsetsDefs,
    isTRUE(attr(x, "hasSubsetDefinitions"))
  ))
}

#' Save cohort subset definitions to json
#' @description
#' This is generally used as part of saveCohortDefinitionSet
#'
#' @param subsetDefinition The subset definition object {@seealso CohortSubsetDefinition}
#'
#' @export
#' @inheritParams saveCohortDefinitionSet
saveCohortSubsetDefinition <- function(subsetDefinition,
                                       subsetJsonFolder = "inst/cohort_subset_definitions/") {
  checkmate::assertR6(subsetDefinition, classes = "CohortSubsetDefinition")

  if (!dir.exists(subsetJsonFolder)) {
    dir.create(subsetJsonFolder, recursive = TRUE)
  }

  ParallelLogger::saveSettingsToJson(
    subsetDefinition$toList(),
    subsetDefinition$getJsonFileName(subsetJsonFolder)
  )
}

#' Get cohort subset definitions from a cohort definition set
#' @description
#' Get the subset definitions (if any) applied to a cohort definition set.
#' Note that these subset definitions are a copy of those applied to the cohort set.
#' Modifying these definitions will not modify the base cohort set.
#' To apply a modification, reapply the subset definition to the cohort definition set data.frame with
#' \code{addCohortSubsetDefinition} with `overwriteExisting = TRUE`.
#'
#' @export
#' @param cohortDefinitionSet  A valid cohortDefinitionSet
#' @returns list of cohort subset definitions or empty list
getSubsetDefinitions <- function(cohortDefinitionSet) {
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))

  if (!hasSubsetDefinitions(cohortDefinitionSet)) {
    return(list())
  }
  subsetDefinitions <- list()
  for (subsetDef in attr(cohortDefinitionSet, "cohortSubsetDefinitions")) {
    subsetDefCopy <- subsetDef$clone(deep = TRUE)
    subsetDefinitions <- c(subsetDefinitions, list(subsetDefCopy))
  }

  return(subsetDefinitions)
}
