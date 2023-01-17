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

#' @title to JSON
#' @description json serialized representation of object
.toJSON <- function(obj) {
  jsonlite::toJSON(obj, pretty = TRUE)
}


#' Endpoint settings
#' @description
#' Representation of an endpoint
#' Based on https://github.com/OHDSI/circe-be/blob/master/src/main/java/org/ohdsi/circe/cohortdefinition/Window.java
#' @export
Endpoint <- R6::R6Class(
  classname = "Endpoint",
  private = list(
    .days = 0,
    .coeff = 0
  ),
  public = list(
        #' @title to List
        #' @description List representation of object
    toList = function() {
      objRepr <- list()
      if (length(private$.days))
        objRepr$days <- jsonlite::unbox(private$.days)
      if (length(private$.coeff))
        objRepr$coeff <- jsonlite::unbox(private$.coeff)

      objRepr
    },
        #' @title to JSON
        #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

        #' @title is Equal to
        #' @description Compare Endpoint to another
        #' @param criteria Endpoint instance
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "Endpoint")
      return(all(self$days == criteria$days,
                 self$coeff == criteria$coeff))
    }
  ),
  active = list(
        #'@field    days Int
    days = function(days) {
      if (missing(days)) return(private$.days)
      checkmate::assertInt(days)
      private$.days <- days
      return(self)
    },
        #'@field  coeff Int
    coeff = function(coeff) {
      if (missing(coeff)) return(private$.coeff)
      checkmate::assertInt(coeff)
      private$.coeff <- coeff
      return(self)
    }
  )
)


#' Window settings
#' @description
#' Representation of a time window
#' Based on https://github.com/OHDSI/circe-be/blob/master/src/main/java/org/ohdsi/circe/cohortdefinition/Window.java
#' @export
Window <- R6::R6Class(
  classname = "Window",
  private = list(
    .start = Endpoint$new(),
    .end = Endpoint$new(),
    .useIndexEnd = FALSE,
    .useEventEnd = FALSE
  ),
  public = list(
        #' @title to List
        #' @description List representation of object
    toList = function() {
      objRepr <- list()
      if (length(private$.start$toList()))
        objRepr$start <- private$.start$toList()
      if (length(private$.end$toList()))
        objRepr$end <- private$.end$toList()
      if (length(private$.useIndexEnd))
        objRepr$useIndexEnd <- jsonlite::unbox(private$.useIndexEnd)
      if (length(private$.useEventEnd))
        objRepr$useEventEnd <- jsonlite::unbox(private$.useEventEnd)

      objRepr
    },
        #' @title to JSON
        #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

        #' @title is Equal to
        #' @description Compare Window to another
        #' @param criteria Window instance
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "Window")
      return(all(self$start == criteria$start,
                 self$end == criteria$end,
                 self$useIndexEnd == criteria$useIndexEnd,
                 self$useEventEnd == criteria$useEventEnd))
    }
  ),
  active = list(
        #'@field start Endpoint
    start = function(start) {
      if (missing(start)) return(private$.start)
      checkmate::assertClass(x = start, classes = "Endpoint")
      private$.start <- start
      return(self)
    },
        #'@field end Endpoint
    end = function(end) {
      if (missing(end)) return(private$.end)
      checkmate::assertClass(x = end, classes = "Endpoint")
      private$.end <- end
      return(self)
    },
        #'@field useIndexEnd Boolean
    useIndexEnd = function(useIndexEnd) {
      if (missing(useIndexEnd)) return(private$.useIndexEnd)
      checkmate::assertLogical(x = useIndexEnd)
      private$.useIndexEnd <- useIndexEnd
      return(self)
    },
        #'@field useEventEnd Boolean
    useEventEnd = function(useEventEnd) {
      if (missing(useEventEnd)) return(private$.useEventEnd)
      checkmate::assertLogical(x = useEventEnd)
      private$.useEventEnd <- useEventEnd
      return(self)
    }
  )
)


#' @title SubsetOperator
#' @description
#' Abstract Base Class for subsets. Subsets should inherit from this and implement their own requirements.
#' @seealso CohortSubsetOperator
#' @seealso DemographicSubsetOperator
#' @seealso LimitSubsetOperator
#'
#' @field name  name of subset operation - should describe what the operation does e.g. "Males under the age of 18", "Exposed to Celecoxib"
#' @field id    id of subset operation - must be unique (to design) int
#'
#' @export
SubsetOperator <- R6::R6Class(
  classname = "SubsetOperator",
  private = list(
    queryBuilder = QueryBuilder,
    suffixStr = "S",
    .name = "",
    .id = NA
  ),

  public = list(
        #' @title Subset class
        #' @description
        #' Subset class - Abstract Base Class, do not implement directly
        #'
        #' @param definition json character or list - definition of subset operator
        #'
        #' @return instance of object
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
        #' @title Class Name
        #' @description Class name of object
    classname = function() {
      class(self)[1]
    },

        #' @title return query builder instance
    getQueryBuilder = function() {
      private$queryBuilder$new(self)
    },
        #' @title Public Fields
        #' @description Publicly settable fields of object
    publicFields = function() {
      return(names(get(self$classname())$active))
    },


        #' @title is Equal to
        #' @description Compare Subsets - are they identical or not?
        #' Checks all fields and settings
        #'
        #' @param  subsetOperatorB A subset to test equivalence to
    isEqualTo = function(subsetOperatorB) {
      checkmate::assertR6(subsetOperatorB, "SubsetOperator")
      if (!all(class(self) == class(subsetOperatorB))) {
        return(FALSE)
      }

      for (field in self$publicFields()) {
        # DemographicCriteriaSubsetOpertior has additional equality test
        if (!is.atomic(self[[field]]))
          next

        if (self[[field]] != subsetOperatorB[[field]])
          return(FALSE)
      }

      return(TRUE)
    },

        #' @title To list
        #' @description convert to List representation
    toList = function() {
      repr <- list(
        id = jsonlite::unbox(private$.id),
        name = jsonlite::unbox(private$.name),
        subsetType = jsonlite::unbox(self$classname())
      )
      return(repr)
    },

        #' @title To Json
        #' @description convert to json serialized representation
        #' @return list representation of object as json character
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


#' @title Cohort Subset
#' @description
#' A subset of type cohort - subset a population to only those contained within defined cohort
#' # TODO - Add the time windowing settings for T/S
#' @export
CohortSubsetOperator <- R6::R6Class(
  classname = "CohortSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    suffixStr = "Coh",
    queryBuilder = CohortSubsetQb,
    .cohortIds = integer(0),
    .cohortCombinationOperator = "all",
    .negate = FALSE,
    .targetCohortWindow = Window$new(),
    .subsetCohortWindow = Window$new()
  ),
  public = list(
            #' @title Public Fields
            #' @description publicly settable fields
    publicFields = function() {
      c(super$publicFields(), "cohortIds", "cohortCombinationOperator", "negate")
    },

            #' @title to List
            #' @description List representation of object
    toList = function() {
      objRepr <- super$toList()
      objRepr$cohortIds <- private$.cohortIds
      objRepr$cohortCombinationOperator <- jsonlite::unbox(private$.cohortCombinationOperator)
      objRepr$negate <- jsonlite::unbox(private$.negate)
      objRepr$targetCohortWindow <- private$.targetCohortWindow$toList()
      objRepr$subsetCohortWindow <- private$.subsetCohortWindow$toList()

      objRepr
    }
  ),
  active = list(
            #'@field cohortIds Integer ids of cohorts to subset to
    cohortIds = function(cohortIds) {
      if (missing(cohortIds))
        return(private$.cohortIds)

      cohortIds <- as.integer(cohortIds)
      # TODO: valid cohorts check. Do we want to allow multiple cohorts or a single cohort?
      # Per https://github.com/OHDSI/CohortGenerator/issues/67#issuecomment-1353624538
      # "Subset cohort x to only patients contained in {y,z}. This can be an any/all for the cohorts in {y,z}"
      checkmate::assertIntegerish(cohortIds, min.len = 1)
      checkmate::assertFALSE(any(is.na(cohortIds)))
      private$.cohortIds <- cohortIds
      self
    },
        #'@field cohortCombinationOperator How to combine the cohorts
    cohortCombinationOperator = function(cohortCombinationOperator) {
      if (missing(cohortCombinationOperator))
        return(private$.cohortCombinationOperator)

      checkmate::assertChoice(x = cohortCombinationOperator, choices = c("any", "all"))
      private$.cohortCombinationOperator <- cohortCombinationOperator
      self
    },
        #'@field negate Inverse the subset rule? TRUE will take the patients NOT in the subset
    negate = function(negate) {
      if (missing(negate))
        return(private$.negate)

      checkmate::assertLogical(x = negate)
      private$.negate <- negate
      self
    },
        #'@field targetCohortWindow The time window to use for the target cohort
    targetCohortWindow = function(targetCohortWindow) {
      if (missing(targetCohortWindow))
        return(private$.targetCohortWindow)

      checkmate::assertClass(x = targetCohortWindow, classes = "Window")
      private$.targetCohortWindow = targetCohortWindow
      self
    },
        #'@field subsetCohortWindow The time window to use for the subset cohort
    subsetCohortWindow = function(subsetCohortWindow) {
      if (missing(subsetCohortWindow))
        return(private$.subsetCohortWindow)

      checkmate::assertClass(x = subsetCohortWindow, classes = "Window")
      private$.subsetCohortWindow = subsetCohortWindow
      self
    }
  )
)

#' A definition of subset functions to be applied to a set of cohorts
#' @param id  unique integer identifier
#' @param name name of operator
#' @param cohortIds integer - set of cohort ids to subset to
#' @returns a CohortSubset instance
#' @export
createCohortSubset <- function(id, name, cohortIds, cohortCombinationOperator, negate, targetCohortWindow, subsetCohortWindow) {
  subset <- CohortSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$cohortIds <- cohortIds
  subset$cohortCombinationOperator <- cohortCombinationOperator
  subset$negate <- negate
  subset$targetCohortWindow <- targetCohortWindow
  subset$subsetCohortWindow <- subsetCohortWindow

  subset
}

#' Demographics settings
#' @description
#' Representation of demographic settings to be used in a subset instance
DemographicCriteria <- R6::R6Class(
  classname = "DemographicCriteria",
  private = list(
    .ageMin = 0,
    .ageMax = 99999,
    .gender = "",
    .race = "",
    .ethnicity = ""
  ),
  public = list(
             #' @title to List
            #' @description List representation of object
    toList = function() {
      objRepr <- list()
      if (length(private$.ageMin))
        objRepr$ageMin <- jsonlite::unbox(private$.ageMin)
      if (length(private$.ageMax))
        objRepr$ageMax <- jsonlite::unbox(private$.ageMax)
      if (length(private$.gender))
        objRepr$gender <- jsonlite::unbox(private$.gender)
      if (length(private$.race))
        objRepr$race <- jsonlite::unbox(private$.race)
      if (length(private$.ethnicity))
        objRepr$ethnicity <- jsonlite::unbox(private$.ethnicity)

      objRepr
    },
            #' @title to JSON
            #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

            #' @title is Equal to
            #' @description Compare Subset to another
            #' @param criteria DemographicCriteria instance
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "DemographicCriteria")
      return(all(self$ageMin == criteria$ageMin,
                 self$ageMax == criteria$ageMax,
                 self$gender == criteria$gender,
                 self$race == criteria$race,
                 self$ethnicity == criteria$ethnicity))
    }
  ),
  active = list(
            #'@field    ageMin Int between 0 and 9999 - minimum age
    ageMin = function(ageMin) {
      if (missing(ageMin)) return(private$.ageMin)
      checkmate::assertInt(ageMin, lower = 0, upper = min(self$ageMax, 99999))
      private$.ageMin <- ageMin
      return(self)
    },
            #'@field  ageMax  Int between 0 and 9999 - maximum age
    ageMax = function(ageMax) {

      if (missing(ageMax)) return(private$.ageMax)
      checkmate::assertInt(ageMax, lower = max(0, self$ageMin), upper = 99999)
      private$.ageMax <- ageMax
      return(self)
    },
            #' @field gender character string denoting gender
    gender = function(gender) {
      if (missing(gender)) return(private$.gender)
      checkmate::assertCharacter(gender, len = 1, null.ok = FALSE)
      private$.gender <- gender
      return(self)
    },
        #' @field race character string denoting race
    race = function(race) {
      if (missing(race)) return(private$.race)
      checkmate::assertCharacter(race, len = 1, null.ok = FALSE)
      private$.race <- race
      return(self)
    },
        #' @field ethnicity character string denoting ethnicity
    ethnicity = function(ethnicity) {
      if (missing(ethnicity)) return(private$.ethnicity)
      checkmate::assertCharacter(ethnicity, len = 1, null.ok = FALSE)
      private$.ethnicity <- ethnicity
      return(self)
    }
  )
)

#' Create demographic criteria
#' @param ageMin       age demographics
#' @param ageMax       age demographics
#' @param gender       gender demographics - concept ID list
#' @param race         race demographics - concept ID list
#' @param ethnicity    ethnicity demographics - concept ID list
#' @export
createDemographicCriteria <- function(ageMin = 0, ageMax = 9999, gender = "", race = "", ethnicity = "") {
  criteria <- DemographicCriteria$new()
  criteria$ageMin <- ageMin
  criteria$ageMax <- ageMax
  criteria$gender <- gender
  criteria$race <- race
  criteria$ethnicity <- ethnicity

  criteria
}

#' Criteria Subset
#' @export
DemographicSubsetOperator <- R6::R6Class(
  classname = "DemographicSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    queryBuilder = DemographicSubsetQb,
    suffixStr = "Dem",
    .criteria = NULL
  ),
  public = list(
            #' @title Public Fields
            #' @description Publicly settable fields of object
    publicFields = function() {
      c(super$publicFields(), "criteria")
    },
            #' @title to List
            #' @description List representation of object
    toList = function() {
      objRef <- super$toList()
      objRef$criteria <- private$.criteria$toList()
      objRef
    },

            #' @title is Equal to
            #' @description Compare Subset to another
            #' @param           subsetOperatorB A subset to test equivalence to
    isEqualTo = function(subsetOperatorB) {
      if (!super$isEqualTo(subsetOperatorB)) {
        return(FALSE)
      }

      return(self$criteria$isEqualTo(subsetOperatorB$criteria))
    }
  ),
  active = list(
            #'@field criteria   DemographicCriteria to subset to
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
#' @export
createDemographicSubset <- function(id, name, ...) {
  subset <- DemographicSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$criteria <- createDemographicCriteria(...)

  subset
}

#' Criteria Subset
#' @export
LimitSubsetOperator <- R6::R6Class(
  classname = "LimitSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    queryBuilder = LimitSubsetQb,
    suffixStr = "Lim",
    .priorTime = 0,
    .followUpTime = 0,
    .limitTo = character(0),
    .calendarStartDate = "",
    .calendarEndDate = ""
  ),
  public = list(
            #' @title to List
            #' @description List representation of object
    toList = function() {
      objRef <- super$toList()
      objRef$priorTime <- jsonlite::unbox(private$.priorTime)
      objRef$followUpTime <- jsonlite::unbox(private$.followUpTime)
      objRef$limitTo <- jsonlite::unbox(private$.limitTo)
      objRef$calendarStartDate <- jsonlite::unbox(private$.calendarStartDate)
      objRef$calendarEndDate <- jsonlite::unbox(private$.calendarEndDate)
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
            #' @field limitTo     character one of:
            #'                              "firstEver" - only first entry in patient history
            #'                              "earliestRemaining" - only first entry after washout set by priorTime
            #'                              "latestRemaining" -  the latest remaining after washout set by followUpTime
            #'                              "lastEver" - only last entry in patient history inside
            #'
            #'                          Note, when using firstEver and lastEver with follow up and washout, patients with events
            #'                          outside this will be censored.
            #'
    limitTo = function(limitTo) {
      if (missing(limitTo))
        return(private$.limitTo)
      checkmate::assertCharacter(limitTo)
      checkmate::assertChoice(limitTo, choices = c("", "firstEver", "earliestRemaining", "latestRemaining", "lastEver"))
      private$.limitTo <- limitTo
      self
    },
        #' @field calendarStartDate            The calendar start date for limiting by date
    calendarStartDate = function(calendarStartDate) {
      if (missing(calendarStartDate))
        return(private$.calendarStartDate)

      checkmate::assertDate(calendarStartDate)
      private$.calendarStartDate <- calendarStartDate
      self
    },
        #' @field calendarEndDate            The calendar end date for limiting by date
    calendarEndDate = function(calendarEndDate) {
      if (missing(calendarEndDate))
        return(private$.calendarEndDate)

      checkmate::assertDate(calendarEndDate)
      private$.calendarEndDate <- calendarEndDate
      self
    }
  )
)

#' Create Limit Subset
#' @description
#' Subset cohorts using specified limit criteria
#'
#'
#' @param priorTime                 Required prior observation window
#' @param followUpTime              Required post observation window
#' @param limitTo           character one of:
#'                              "firstEver" - only first entry in patient history
#'                              "earliestRemaining" - only first entry after washout set by priorTime
#'                              "latestRemaining" -  the latest remaining after washout set by followUpTime
#'                              "lastEver" - only last entry in patient history inside
#'
#'                          Note, when using firstEver and lastEver with follow up and washout, patients with events
#'                          outside this will be censored. The "firstEver" and "lastEver" are applied first. 
#'                          The "earliestRemaining" and "latestRemaining" are applied after all other limit 
#'                          criteria are applied (i.e. after applying prior/post time and calendar time).
#'                    
#' @export
createLimitSubset <- function(id, name, priorTime, followUpTime, limitTo) {
  subset <- LimitSubsetOperator$new()
  subset$id <- id
  subset$name <- name
  subset$priorTime <- priorTime
  subset$followUpTime <- followUpTime
  subset$limitTo <- limitTo

  subset
}


#' @title Cohort Subset Definition
#' @description
#' Set of subset definitions
#' @export
CohortSubsetDefinition <- R6::R6Class(
  classname = "CohortSubsetDefinition",
  private = list(
    .name = "",
    .definitionId = integer(0),
    .subsets = list(),
    .subsetIds = c(),
    .targetOutcomePairs = c(),

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
        #' @title initialize
        #' @param definition  json or list representation of object
    initialize = function(definition = NULL) {
      if (!is.null(definition)) {
        definition <- .loadJson(definition)
        self$name <- definition$name
        self$definitionId <- definition$definitionId
        self$targetOutcomePairs <- definition$targetOutcomePairs
        self$subsets <- lapply(definition$subsets, private$createSubset)
        self$targetCohortWindow <- definition$targetCohortWindow
        self$subsetCohortWindow <- definition$subsetCohortWindow
      }
      self
    },
        #' @title to List
        #' @description List representation of object
    toList = function() {
      list(
        name = jsonlite::unbox(self$name),
        definitionId = jsonlite::unbox(self$definitionId),
        targetOutcomePairs = self$targetOutcomePairs,
        # Note - when there is a base definition that includes multiple calls to the same subset this should be replaced
        subsets = lapply(self$subsets, function(subset) { subset$toList() }),
        subsetOperatorIds = private$.subsetIds,
        packageVersion = jsonlite::unbox(as.character(utils::packageVersion(utils::packageName())))
      )
    },
        #' @title to JSON
        #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

        #' @title add Subset Operator
        #' @description add subset to class - checks if equivalent id is present
        #' Will throw an error if a matching ID is found but reference object is different
        #' @param subsetOperator a SubsetOperator isntance
        #' @param overwrite if a subset operator of the same ID is present, replace it with a new definition
    addSubsetOperator = function(subsetOperator) {
      checkmate::assertR6(subsetOperator, "SubsetOperator")
      existingOperator <- self$getSubsetOperatorById(!subsetOperator$id)
      if (is.null(existingOperator)) {
        private$.subsets <- c(private$.subsets, subsetOperator)
        private$.subsetIds <- c(private$.subsetIds, subsetOperator$id)
      } else if (subsetOperator$isEqualTo(existingOperator)) {
        stop("Non-equivalent subset operator with the same id is present in definition.")
      }
      self
    },

        #' @title Get SubsetOperator By Id
        #' @description get a subset operator by its id field
        #' @param id    Integer subset id
    getSubsetOperatorById = function(id) {
      # This implementation seems weird but if you store int ids in a list then R will store every int lower than that
      # Value as a NULL, which breaks any calls to "x %in% names(listObj)"
      if (!id %in% private$.subsetId) {
        return(NULL)
      }

      for (subset in private$.subsets) {
        if (subset$id == id)
          return(subset)
      }
    },

    #' @title get query parts
    #'
    #' Returns vector of join, logic, having statments returned by subset operations
    getQueryParts = function() {
      results <- lapply(self$subsets, function(subset) {
        subset$getQueryBuilder()$getQueryParts()
      })

      combinedList <- list()
      for (result in results) {
        for (key in c("logic", "havingClauses", "joins")) {
          combinedList[[key]] <- c(combinedList[[key]], result[[key]])
        }
      }

      # Allow multiple having clauses across table object ids
      # NOTE - this logic could be more complex (e.g. 3 entries from cohort 1 OR 2 entries from cohort 2)
      if (length(combinedList$havingClauses) >= 1)
       combinedList$havingClauses <- paste("HAVING", paste(combinedList$havingClauses, collapse = " AND "))

      return(combinedList)
    },
  ),

  active = list(
        #' @field targetOutcomePairs  list of pairs of intgers - (targetCohortId, outputCohortId)
    targetOutcomePairs = function(targetOutcomePairs) {
      if (missing(targetOutcomePairs))
        return(private$.targetOutcomePairs)
      checkmate::assertList(targetOutcomePairs, types = c("numeric", "list"), min.len = 1, unique = TRUE)

      targetOutcomePairs <- lapply(targetOutcomePairs,
                                   function(targetOutcomePair) {
                                     targetOutcomePair <- as.numeric(targetOutcomePair)
                                     checkmate::assertIntegerish(targetOutcomePair, len = 2)
                                     checkmate::assertFALSE(targetOutcomePair[[1]] == targetOutcomePair[[2]])
                                     targetOutcomePair
                                   })

      private$.targetOutcomePairs <- targetOutcomePairs
      self
    },
        #' @field subsets list of subset operations
    subsets = function(subsets) {
      if (missing(subsets))
        return(private$.subsets)

      checkmate::assertList(subsets, types = "SubsetOperator")
      lapply(subsets, self$addSubsetOperator)
      self
    },

    name = function(name) {
      if (missing(name))
        return(private$.name)

      checkmate::assertCharacter(name)
      private$.name <- name
      self
    },

    definitionId = function(definitionId) {
      if (missing(definitionId))
        return(private$.definitionId)

      checkmate::assertInt(definitionId)
      private$.definitionId <- definitionId
      self
    },

    subsetIds = function(subsetIds) {
      if (missing(subsetIds))
        return(private$.subsetIds)

      checkmate::assertVector(subsetIds, min.len = 1, unique = TRUE)
      private$.subsetIds <- subsetIds
      self
    }
  )
)

#' Create Subset Definition
#' @description
#' Create subset definition from subset objects
#' @param name                      Name of definition
#' @param definitionId              Definition identifier
#' @param targetOutcomePairs        Vector of pairs targetCohortId, outcomeCohortId
#' @param subsets                   vector of subset instances to apply
#' @export
#'
#' @examples
#' mySubsets <- c(
#'  createCohortSubset(id = 12, name = "foo", cohortJson = cohortDefinition)
#' )
#'
#' subsetDef <- createSubsetDefinition(c(1,3,4), 11, mySubsets)
createCohortSubsetDefinition <- function(name, definitionId, targetOutcomePairs, subsets) {
  subsetDef <- CohortSubsetDefinition$new()
  subsetDef$name <- name
  subsetDef$definitionId <- definitionId
  subsetDef$targetOutcomePairs <- targetOutcomePairs
  subsetDef$subsets <- subsets
  return(subsetDef)
}


#' Add cohort subset definition to a cohort definition set
#' @description
#' Given a subset definition and cohort definition set, this function returns a modified cohortDefinitionSet
#' That contains cohorts that's have parent's contained within the base cohortDefinitionSet
#'
#' Also adds the columns subsetParent and isSubset that denote if the cohort is a subset and what the parent definition
#' is.
#'
#'
#' @param cohortDefinitionSet       data.frame that conforms to CohortDefinitionSet
#' @param cohortSubsetDefintion     CohortSubsetDefinition instance
#'
#' @export
addCohortSubsetDefinition <- function(cohortDefinitionSet, cohortSubsetDefintion) {
  checkmate::assertR6(cohortSubsetDefintion, "CohortSubsetDefinition")
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))

  # DEV NOTE: In principle, this function could be be applied recursively to any existing subsets if such behaviour is desired,
  # however - this would require functionality that checks to see if parent cohorts have already been generated and places
  # order restrictions on tob - these if statments are commented
  # if (!"subsetParent" %in% colnames(cohortDefinitionSet))
  cohortDefinitionSet$subsetParent <- cohortDefinitionSet$cohortId

  #if (!"isSubset" %in% colnames(cohortDefinitionSet))
  cohortDefinitionSet$isSubset <- FALSE

  queryParts <- cohortSubsetDefintion$getQueryParts()

  for (targetOutcomePair in cohortSubsetDefintion$targetOutcomePairs) {
    if (!targetOutcomePair[1] %in% cohortDefinitionSet$cohortId) {
      stop("Target cohortid ", targetOutcomePair[1], " not found in cohort definition set")
    }

    if (targetOutcomePair[2] %in% cohortDefinitionSet$cohortId) {
      stop("Outcome cohort id ", targetOutcomePair[2], " found in cohort definition set - must be unique indentifier")
    }
    # Get ordered set of clauses from subset operation definitions

    # REQUIRED PARAMETERS:
    # --  @target_cohort_id
    # --  @output_cohort_id
    # -- OPTIONAL PARAMETERS
    # {DEFAULT @join_statements = ''}
    # {DEFAULT @and_clauses = ''}
    # {DEFAULT @having_clauses = ''}
    subsetSql <- SqlRender::loadRenderTranslateSql(file.path("subsets", "CohortSubsetDefinition.sql"),
                                                   target_cohort_id = targetOutcomePair[1],
                                                   output_cohort_id = targetOutcomePair[2],
                                                   join_statments = queryParts$joins,
                                                   and_clauses = queryParts$logic,
                                                   having_clauses = queryParts$havingClauses,
                                                   packageName = "CohortGenerator")

    cohortDefinitionSet <- cohortSubsetDefintion %>%
      dplyr::bind_rows(c(cohortId = targetOutcomePair[2],
                         subsetParent = targetOutcomePair[1],
                         cohortName = subsetCohortName,
                         sql = subsetSql,
                         json = "{}"))
  }

  attr(cohortDefinitionSet, "hasSubsetDefinitions") <- TRUE
  return(cohortDefinitionSet)
}
