# Copyright 2022 Observational Health Data Sciences and Informatics
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

.loadJson <- function(definition, simplifyVector = FALSE, simplifyDataFrame = FALSE, ...) {
  if (is.character(definition)) {
    definition <- jsonlite::fromJSON(definition,
                                     simplifyVector = simplifyVector,
                                     simplifyDataFrame = simplifyDataFrame,
                                     ...)
  }

  if (!is.list(definition)) {
    stop("Cannot instanitate object invalid type ", class(definition))
  }
  definition
}

.toJSON <- function(obj) {
  jsonlite::toJSON(obj, pretty = TRUE)
}

#' Subset class
#' @description
#' Abstract Base Class for subsets. Subsets should inherit from this and implement their own requirements.
#' @seealso CohortSubsetOperator
#' @seealso DemographicSubsetOperator
#' @seealso LimitSubsetOperator
#'
#' @export
SubsetOperator <- R6::R6Class(
  classname = "SubsetOperator",
  private = list(
    .name = "",
    .id = NA
  ),

  public = list(
    initialize = function(definition = NULL) {
      if (!is.null(definition)) {
        definition <- .loadJson(definition)

        for (field in names(definition)) {
          if (field %in% self$publicFields()) {
            self[[field]] <- definition[[field]]
          }
        }
      }
      self
    },

    classname = function() {
      class(self)[1]
    },
    publicFields = function() {
      return(names(get(self$classname())$active))
    },

        #' Compare Subset
        #' @param           subsetOperatorB A subset to test equivalence to
    isEqualTo = function(subsetOperatorB) {
      checkmate::assertR6(subsetOperatorB, "SubsetOperator")
      if (!all(class(self) == class(subsetOperatorB))) {
        return(FALSE)
      }

      for (field in self$publicFields()) {
        # DemographicCriteriaSubsetOperation has additional eqaulity test
        if (!is.atomic(self[[field]]))
          next

        if (self[[field]] != subsetOperatorB[[field]])
          return(FALSE)
      }

      return(TRUE)
    },

    toList = function() {
      repr <- list(
        id = jsonlite::unbox(private$.id),
        name = jsonlite::unbox(private$.name),
        subsetType = jsonlite::unbox(self$classname())
      )
      return(repr)
    },

    toJSON = function() {
      .toJSON(self$toList())
    }
  ),

  active = list(
    id = function(id) {
      if (missing(id))
        return(private$.id)

      checkmate::assertInt(id)
      private$.id <- id
      self
    },

    name = function(name) {
      if (missing(name))
        return(private$.name)

      checkmate::assertCharacter(name)
      private$.name <- name
      self
    }
  )
)


#' Cohort Subset
#' @description
#' A subset of type cohort - subset a population to only those contained within defined cohort
#' @export
CohortSubsetOperator <- R6::R6Class(
  classname = "CohortSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    .cohortIds = integer(0)
  ),
  public = list(
    publicFields = function() {
      c(super$publicFields(), "cohortIds")
    },

    toList = function() {
      objRepr <- super$toList()
      objRepr$cohortIds <- private$.cohortIds
      objRepr
    }
  ),
  active = list(
                #'@field cohortJson             circe cohort definition json
    cohortIds = function(cohortIds) {
      if (missing(cohortIds))
        return(private$.cohortIds)

      cohortIds <- as.integer(cohortIds)
      # TODO: valid cohorts check. Do we want to allow multiple cohorts or a single cohort?
      checkmate::assertIntegerish(cohortIds, min.len = 1)
      checkmate::assertFALSE(any(is.na(cohortIds)))
      private$.cohortIds <- cohortIds
      self
    }
  )
)

#' A definition of subset functions to be applied to a set of cohorts
#' @param id
#' @param name
#' @param cohortIds
#' @returns a CohortSubset instance
#' @export
createCohortSubset <- function(id, name, cohortIds) {
  subset <- CohortSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$cohortIds <- cohortIds

  subset
}

#' Demographics settings
#' @description
#' Representation of demographic settings to be used in a subset instance
#'
DemographicCriteria <- R6::R6Class(
  classname = "DemographicCriteria",
  private = list(
    .ageMin = 0,
    .ageMax = 99999,
    .gender = ""
  ),
  public = list(
    toList = function() {
      objRepr <- list()
      if (length(private$.ageMin))
        objRepr$ageMin <- jsonlite::unbox(private$.ageMin)
      if (length(private$.ageMax))
        objRepr$ageMax <- jsonlite::unbox(private$.ageMax)
      if (length(private$.gender))
        objRepr$gender <- jsonlite::unbox(private$.gender)

      objRepr
    },

    toJSON = function() {
      .toJSON(self$toList())
    },

    # Are two demographic criteria functionally identical
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "DemographicCriteria")
      return(all(self$ageMin == criteria$ageMin,
                 self$ageMax == criteria$ageMax,
                 self$gender == criteria$gender))
    }
  ),
  active = list(
                #'@field    ageMin
    ageMin = function(ageMin) {
      if (missing(ageMin)) return(private$.ageMin)
      checkmate::assertInt(ageMin, lower = 0, upper = min(self$ageMax, 99999))
      private$.ageMin <- ageMin
      return(self)
    },
                #'@field    ageMax
    ageMax = function(ageMax) {
      if (missing(ageMax)) return(private$.ageMax)
      checkmate::assertInt(ageMax, lower = max(0, self$ageMin), upper = 99999)
      private$.ageMax <- ageMax
      return(self)
    },
               #' @field gender
    gender = function(gender) {
      if (missing(gender)) return(private$.gender)
      checkmate::assertCharacter(gender, len = 1, null.ok = FALSE)
      private$.gender <- gender
      return(self)
    }
  )
)

#' Create demographic criteria
#' @param ageMin       age demographics
#' @param ageMax       age demographics
#' @param gender       gender demographics
#' # TODO: more criteria than this - calendarYearMin/Max, what else?
#' @export
createDemographicCriteria <- function(ageMin = 0, ageMax = 9999, gender = "") {
  criteria <- DemographicCriteria$new()
  criteria$ageMin <- ageMin
  criteria$ageMax <- ageMax
  criteria$gender <- gender

  criteria
}

#' Criteria Subset
#' @export
DemographicSubsetOperator <- R6::R6Class(
  classname = "DemographicSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    .criteria = NULL
  ),
  public = list(
    publicFields = function() {
      c(super$publicFields(), "criteria")
    },

    toList = function() {
      objRef <- super$toList()
      objRef$criteria <- private$.criteria$toList()
      objRef
    },

        #' Compare Subset to another
        #' @param           subsetOperatorB A subset to test equivalence to
    isEqualTo = function(subsetOperatorB) {
      if (!super$isEqualTo(subsetOperatorB)) {
        return(FALSE)
      }

      return(self$criteria$isEqualTo(subsetOperatorB$criteria))
    }
  ),
  active = list(
                #'@field criteia             criteria
    criteria = function(criteria) {
      if (missing(criteria))
        return(private$.criteria)

      # Allows criteria definition to be loaded from serialized form
      if (is.list(criteria)) {
        criteria <- do.call(createDemographicCriteria, criteria)
      }

      checkmate::assertR6(criteria, "DemographicCriteria")
      private$.criteria <- criteria
      self
    }
  )
)

#' Create DemographicCriteria Subset
#' @param id            Id number
#' @param name          char name
#' @param ...           Demographic criteria @seealso createDemographicCriteria
createDemographicSubset <- function(id, name, ...) {
  subset <- DemographicSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$criteria <- createDemographicCriteria(...)

  subset
}

#' Criteria Subset
#'
#' @export
LimitSubsetOperator <- R6::R6Class(
  classname = "LimitSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    .priorTime = 0,
    .followUpTime = 0,
    .limitTo = character(0)
  ),
  public = list(
    toList = function() {
      objRef <- super$toList()
      objRef$priorTime <- jsonlite::unbox(private$.priorTime)
      objRef$followUpTime <- jsonlite::unbox(private$.followUpTime)
      objRef$limitTo <- jsonlite::unbox(private$.limitTo)
      objRef
    }
  ),
  active = list(
                #' @field priorTime             minimum washout time in days
    priorTime = function(priorTime) {
      if (missing(priorTime))
        return(private$.priorTime)

      checkmate::assertInt(priorTime, lower = 0, upper = 999999)
      private$.priorTime <- priorTime
      self
    },
                #' @field followUpTime            minimum required follow up time in days
    followUpTime = function(followUpTime) {
      if (missing(followUpTime))
        return(private$.followUpTime)

      checkmate::assertInt(followUpTime, lower = 0, upper = 999999)
      private$.priorTime <- followUpTime
      self
    },
    #' @field limitTo           Limit to first observation only
    #'
    #' @param limitTo           charachter one of:
    #'                              "earliest" - only first entry in patient history
    #'                              "earliestRemaining" - only first entry after washout set by priorTime
    #'                              "last" - only last entry in patient history
    #'                              "lastRemaining" - only last entry in patient history inside
    #'
    #'                          Note, when using earliest and last with follow up and washout, patients with events
    #'                          outside this will be censored.
    #'
    limitTo = function(limitTo) {
      if (missing(limitTo))
        return(private$.limitTo)
      checkmate::assertCharacter(limitTo)
      checkmate::assertChoice(limitTo, choices = c("", "earliest", "earliestRemaining", "last", "lastRemaining"))
      private$.limitTo <- limitTo
      self
    }
  )
)

#' Create Observation Criteria Subset
#' @description
#' Subset cohorts using specificed observation criteria
#'
#' @inheritParams createDemographicCriteriaSubset
#'
#' @param priorTime                 Required prior observation window
#' @param followUpTime              Required post observation window
#' @param first                     limit to first entry only
#' @param last                      limit to last entry only
#' @param random                    limit to random entry only
createLimitSubset <- function(id, name, priorTime, followUpTime, limitTo) {
  subset <- LimitSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$priorTime <- priorTime
  subset$followUpTime <- followUpTime
  subset$limitTo <- limitTo

  subset
}


#' Set of subset definitions
#' @export
CohortSubsetDefinition <- R6::R6Class(
  classname = "CohortSubsetDefinition",
  private = list(
    .subsets = list(),
    .subsetIds = c(),
    .targetCohortIds = integer(0),
    .outputCohortId = integer(0),

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
    initialize = function(definition = NULL) {
      if (!is.null(definition)) {
        definition <- .loadJson(definition)
        self$targetCohortIds <- as.integer(definition$targetCohortIds)
        self$outputCohortId <- definition$outputCohortId
        self$subsets <- lapply(definition$subsets, private$createSubset)
      }
      self
    },

    toList = function() {
      list(
        targetCohortIds = as.integer(private$.targetCohortIds),
        subsets = lapply(self$subsets, function(subset) { subset$toList() }),
        outputCohortId = jsonlite::unbox(private$.outputCohortId)
      )
    },

    toJSON = function() {
      .toJSON(self$toList())
    },

    addSubsetOperator = function(subsetOperator) {
      checkmate::assertR6(subsetOperator, "SubsetOperator")
      if (!subsetOperator$id %in% private$.subsetsIds) {
        private$.subsets <- c(private$.subsets, subsetOperator)
        private$.subsetIds <- c(private$.subsetIds, subsetOperator$id)
      } else {
        # TODO Check if subset is equivalent to existing with the same ID or throw error
      }
      self
    }
  ),

  active = list(
    #' @field cohortIds
    outputCohortId = function(outputCohortId) {
      if (missing(outputCohortId))
        return(private$.outputCohortId)

      checkmate::assertInt(outputCohortId)
      private$.outputCohortId <- outputCohortId
      self
    },

        #' @field cohortIds
    targetCohortIds = function(targetCohortIds) {
      if (missing(targetCohortIds))
        return(private$.targetCohortIds)
      checkmate::assertIntegerish(targetCohortIds, min.len = 1)
      checkmate::assertFALSE(any(is.na(targetCohortIds)))
      private$.targetCohortIds <- targetCohortIds
      self
    },
        #' @field subsets
    subsets = function(subsets) {
      if (missing(subsets))
        return(private$.subsets)

      checkmate::assertList(subsets, types = "SubsetOperator")
      lapply(subsets, self$addSubsetOperator)
      self
    }
  )
)

#' Create Subset Definition
#' @description
#' Create subset definition from subset objects
#'
#' @param targetCohortIds                 cohort identifiers to apply subset functions to
#' @param subsets                   vector of subset instances to apply
#' @export
#'
#' @example
#' mySubsets <- c(
#'  createCohortSubset(id = 12, name = "foo", cohortJson = cohortDefinition)
#' )
#'
#' subsetDef <- createSubsetDefinition(c(1,3,4), 11, mySubsets)
createCohortSubsetDefinition <- function(targetCohortIds, outputCohortId, subsets) {
  subsetDef <- CohortSubsetDefinition$new()
  subsetDef$targetCohortIds <- targetCohortIds
  subsetDef$outputCohortId <- outputCohortId
  subsetDef$subsets <- subsets
  return(subsetDef)
}