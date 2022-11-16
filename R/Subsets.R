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


#' Subset class
#' @description
#' Abstract Base Class for subsets. Subsets should inherit from this and implement their own requirements.
#'
#'
Subset <- R6::R6Class(
  classname = "Subset",
  private = list(
    .name = "",
    .id = NA
  ),
  public = list(
    toList = function() {
      list(
        id = jsonlite::unbox(private$.id),
        name = jsonlite::unbox(private$.name),
        type = jsonlite::unbox(class(self)[1])
      )
    },

    toJSON = function () {
      ParallelLogger::convertSettingsToJson(self$toList())
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
CohortSubset <- R6::R6Class(
  classname = "CohortSubset",
  inherit = Subset,
  private = list(
    .cohortJson = character(0)
  ),
  public = list(
    toList = function() {
      objRepr <- super$toList()
      objRepr$cohortJson <- jsonlite::unbox(private$.cohortJson)
      objRepr
    }
  ),
  active = list(
    #'@field cohortJson             circe cohort definition json
    cohortJson = function(cohortJson) {
      if(missing(cohortJson))
        return(private$.cohortJson)

      # TODO: valid json check
      checkmate::assertCharacter(cohortJson)
      private$.cohortJson <- cohortJson
      self
    }
  )
)

#' A definition of subset functions to be applied to a set of cohorts
#' @param id
#' @param name
#' @param cohortJson
#' @returns a CohortSubset instance
#' @export
createCohortSubset <- function(id, name, cohortJson) {
  subset <- CohortSubset$new()
  subset$id <- id
  subset$name <- name
  subset$cohortJson <- cohortJson

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
    .gender = character(0)
  ),
  public = list(
    toList = function () {
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
      ParallelLogger::convertSettingsToJson(self$toList())
    }
  ),
  active = list(
    #'@field    ageMin
    ageMin = function(ageMin) {
      if(missing(ageMin)) return(private$.ageMin)
      checkmate::assertInt(ageMin, lower = 0, upper = 99999)
      private$.ageMin <- ageMin
      return(self)
    },
    #'@field    ageMax
    ageMax = function(ageMax) {
      if(missing(ageMax)) return(private$.ageMax)
      checkmate::assertInt(ageMax, lower = 0, upper = 99999)
      private$.ageMax <- ageMax
      return(self)
    },
    #' @field gender
    gender = function(gender) {
      if(missing(gender)) return(private$gender)
      checkmate::assertCharacter(gender, min.chars = 1, len = 1, null.ok = TRUE)
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
createDemographicCriteria <- function(ageMin = 0, ageMax = 9999, gender = NULL) {
  criteria <- DemographicCriteria$new()
  criteria$ageMin <- ageMin
  criteria$ageMax <- ageMax
  criteria$gender <- gender

  criteria
}

#' Criteria Subset
#' @export
DemographicCriteriaSubset <- R6::R6Class(
  classname = "DemographicCriteriaSubset",
  inherit = Subset,
  private = list(
    .criteria = NULL,
    .atLeast = logical(0)
  ),
  public = list(
    toList = function() {
      objRef <- super$toList()
      objRef$criteria <- private$.criteria$toList()
      objRef
    }
  ),
  active = list(
    #'@field criteia             criteria
    criteria = function(criteria) {
      if(missing(criteria))
        return(private$.criteria)

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
createDemographicCriteriaSubset <- function(id, name, ...) {
  subset <- DemographicCriteriaSubset$new()
  subset$id <- id
  subset$name <- name
  subset$criteria <- createDemographicCriteria(...)
}

#' Criteria Subset
#'
#' @export
ObservationCriteriaSubset <- R6::R6Class(
  classname = "ObservationCriteriaSubset",
  inherit = Subset,
  private = list(
    .priorTime = 0,
    .followUpTime = 0,
    .first = FALSE,
    .last = FALSE,
    .random = FALSE
  ),
  public = list(
    toList = function() {
      objRef <- super$toList()
      objRef$priorTime <- jsonlite::unbox(private$.priorObservation)
      objRef$followUpTime <- jsonlite::unbox(private$.priorObservation)
      objRef
    }
  ),
  active = list(
    #' @field priorTime             minimum washout time in days
    priorTime = function(priorTime) {
      if(missing(priorTime))
        return(private$.followUpTimepriorTime)

      checkmate::assertInt(priorTime, lower = 0, upper = 999999)
      private$.priorTime <- priorTime
      self
    },
    #' @field followUpTime            minimum required follow up time in days
    followUpTime = function(followUpTime) {
      if(missing(followUpTime))
        return(private$.followUpTime)

      checkmate::assertInt(followUpTime, lower = 0, upper = 999999)
      private$.priorTime <- followUpTime
      self
    },
    #' @field first             Limit to first observation only
    first = function(first) {
      if(missing(first))
        return(private$.first)

      if (any(private$.last, private$.random) & first)
        warning("Overriding last/random observation")

      private$.last <- FALSE
      private$.random <- FALSE

      checkmate::assertLogical(first, len = 1)
      private$.first <- first
      self
    },

    #' @field last             Limit to last observation only
    last = function(last) {
      if(missing(last))
        return(private$.last)

      if (any(private$.first, private$.random) & last)
        warning("Overriding first/random observation")

      private$.first <- FALSE
      private$.random <- FALSE

      checkmate::assertLogical(last, len = 1)
      private$.last <- last
      self
    },

    #' @field random             Limit to a random observation only
    random = function(random) {
      if(missing(random))
        return(private$.random)

      checkmate::assertLogical(random, len = 1)
      private$.random <- random
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
createObservationCriteriaSubset <- function(id, name, priorTime, followUpTime, first, last, random) {
  subset <- ObservationCriteriaSubset$new()
  subset$id <- id
  subset$name <- name
  subset$priorTime <- priorTime
  subset$followUpTime <- followUpTime
  subset$first <- first
  subset$last <- last
  subset$random <- random
}


#' Set of subset definitions
#' @export
SubsetDefinition <- R6::R6Class(
  classname = "SubsetDefinition",
  private = list(
    .subsets = list(),
    .cohortIds = NULL,
    getSubsetList = function(subset) {
      res <- list()
      for (s in names(private$.subsets)) {
        subset <- private$.subsets[[s]]
        res[[s]] <- subset$toList()
      }
      res
    }
  ),
  public = list(
    #'
    toList = function() {
      list(
        cohortIds = private$.cohortIds,
        subsets = private$getSubsetList()
      )
    },

    toJSON = function() {
      return(ParallelLogger::convertSettingsToJson(self$toList()))
    }
  ),

  active = list(
    #' @field cohortIds
    cohortIds = function(cohortIds) {
      if(missing(cohortIds))
        return(private$.cohortIds)

      checkmate::assertIntegerish(cohortIds, min.len = 1)
      private$.cohortIds <- cohortIds
      self
    },
    #' @field subsets
    subsets = function(subsets) {
      if (missing(subsets))
        return(private$.subsets)

      checkmate::assertList(subsets)
      for (s in names(subsets)) {
        checkmate::assertR6(subsets[[s]], "Subset")
      }
      # comment allows appending subsets to existing rather than setting
      # private$.subsets <- c(private$.subsets, subsets)
      private$.subsets <- subsets
      self
    }
  )
)

#' Create Subset Definition
#' @description
#' Create subset definition from subset objects
#'
#' @param cohortIds                 cohort identifiers to apply subset functions to
#' @param subsets                   vector of subset instances to apply
#' @export
#'
#' @example
#' mySubsets <- c(
#'  createCohortSubset(id = 12, name = "foo", cohortJson = cohortDefinition)
#' )
#'
#' subsetDef <- createSubsetDefinition(c(1,3,4), mySubsets)
createSubsetDefinition <- function(cohortIds, subsets) {
  subsetDef <- SubsetDefinition$new()
  subsetDef$cohortIds <- cohortIds
  subsetDef$subsets <- subsets
  return(subsetDef)
}