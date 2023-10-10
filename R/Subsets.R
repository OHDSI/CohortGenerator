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

.loadJson <- function(definition, simplifyVector = FALSE, simplifyDataFrame = FALSE, ...) {
  if (is.character(definition)) {
    definition <- jsonlite::fromJSON(definition,
      simplifyVector = simplifyVector,
      simplifyDataFrame = simplifyDataFrame,
      ...
    )
  }

  if (!is.list(definition)) {
    stop("Cannot instanitate object invalid type ", class(definition))
  }
  definition
}

.toJSON <- function(obj) {
  jsonlite::toJSON(obj, pretty = TRUE)
}


# SubsetCohortWindow -------------
#' SubsetCohortWindow settings
#' @export
#' @description
#' Representation of a time window to use when subsetting a target cohort with a subset cohort
SubsetCohortWindow <- R6::R6Class(
  classname = "SubsetCohortWindow",
  private = list(
    .startDay = as.integer(0),
    .endDay = as.integer(0),
    .targetAnchor = "cohortStart"
  ),
  public = list(
    #' @title to List
    #' @description List representation of object
    toList = function() {
      objRepr <- list()
      if (length(private$.startDay)) {
        objRepr$startDay <- jsonlite::unbox(private$.startDay)
      }
      if (length(private$.endDay)) {
        objRepr$endDay <- jsonlite::unbox(private$.endDay)
      }
      if (length(private$.targetAnchor)) {
        objRepr$targetAnchor <- jsonlite::unbox(private$.targetAnchor)
      }

      objRepr
    },
    #' To JSON
    #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },

    #' Is Equal to
    #' @description Compare SubsetCohortWindow to another
    #' @param criteria SubsetCohortWindow instance
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "SubsetCohortWindow")
      return(all(
        self$startDay == criteria$startDay,
        self$endDay == criteria$endDay,
        self$targetAnchor == criteria$targetAnchor
      ))
    }
  ),
  active = list(
    #' @field startDay Integer
    startDay = function(startDay) {
      if (missing(startDay)) {
        return(private$.startDay)
      }
      checkmate::assertIntegerish(x = startDay)
      private$.startDay <- as.integer(startDay)
      return(self)
    },
    #' @field endDay Integer
    endDay = function(endDay) {
      if (missing(endDay)) {
        return(private$.endDay)
      }
      checkmate::assertIntegerish(x = endDay)
      private$.endDay <- as.integer(endDay)
      return(self)
    },
    #' @field targetAnchor Boolean
    targetAnchor = function(targetAnchor) {
      if (missing(targetAnchor)) {
        return(private$.targetAnchor)
      }
      checkmate::assertChoice(x = targetAnchor, choices = c("cohortStart", "cohortEnd"))
      private$.targetAnchor <- targetAnchor
      return(self)
    }
  )
)

# createSubsetCohortWindow ------------------------------
#' A definition of subset functions to be applied to a set of cohorts
#' @export
#' @param startDay  The start day for the window
#' @param endDay The end day for the window
#' @param targetAnchor To anchor using the target cohort's start date or end date
#' @returns a SubsetCohortWindow instance
createSubsetCohortWindow <- function(startDay, endDay, targetAnchor) {
  window <- SubsetCohortWindow$new()
  window$startDay <- startDay
  window$endDay <- endDay
  window$targetAnchor <- targetAnchor
  window
}

# SubsetOperator ------------------------------
#' @title SubsetOperator
#' @export
#' @description
#' Abstract Base Class for subsets. Subsets should inherit from this and implement their own requirements.
#' @seealso CohortSubsetOperator
#' @seealso DemographicSubsetOperator
#' @seealso LimitSubsetOperator
#'
#' @field name  name of subset operation - should describe what the operation does e.g. "Males under the age of 18", "Exposed to Celecoxib"
#'
SubsetOperator <- R6::R6Class(
  classname = "SubsetOperator",
  private = list(
    queryBuilder = QueryBuilder,
    suffixStr = "S",
    .name = NULL,
    baseFields = c("name")
  ),
  public = list(
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
    #' Class Name
    #' @description Class name of object
    classname = function() {
      class(self)[1]
    },

    #' Get auto generated name
    #' @description
    #' Not intended to be used - should be implemented in subclasses
    getAutoGeneratedName = function() {
      return(private$suffixStr)
    },

    #' Return query builder instance
    #' @param id - integer that should be unique in the sql (e.g. increment it by one for each subset operation in set)
    #' @description Return query builder instance
    getQueryBuilder = function(id) {
      private$queryBuilder$new(self, id)
    },
    #' Public Fields
    #' @description Publicly settable fields of object
    publicFields = function() {
      # Note that this will probably break if you subclass a subclass
      return(c(private$baseFields, names(get(self$classname())$active)))
    },

    #' Is Equal to
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
        if (!is.atomic(self[[field]])) {
          next
        }

        if (is.null(self[[field]]) & is.null(subsetOperatorB[[field]])) {
          next
        }

        if (is.null(self[[field]]) & !is.null(subsetOperatorB[[field]])) {
          return(FALSE)
        }

        if (!is.null(self[[field]]) & is.null(subsetOperatorB[[field]])) {
          return(FALSE)
        }

        if (self[[field]] != subsetOperatorB[[field]]) {
          return(FALSE)
        }
      }

      return(TRUE)
    },

    #' To list
    #' @description convert to List representation
    toList = function() {
      repr <- list(
        name = jsonlite::unbox(self$name),
        subsetType = jsonlite::unbox(self$classname())
      )
      return(repr)
    },

    #' To Json
    #' @description convert to json serialized representation
    #' @return list representation of object as json character
    toJSON = function() {
      .toJSON(self$toList())
    }
  ),
  active = list(
    name = function(name) {
      if (missing(name)) {
        if (!is.null(private$.name)) {
          return(private$.name)
        }

        return(self$getAutoGeneratedName())
      }
      checkmate::assertCharacter(name, null.ok = TRUE)
      private$.name <- name
      self
    }
  )
)


# CohortSubsetOperator ------------------------------
#' @title Cohort Subset Operator
#' @export
#' @description
#' A subset of type cohort - subset a population to only those contained within defined cohort
CohortSubsetOperator <- R6::R6Class(
  classname = "CohortSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    suffixStr = "Coh",
    queryBuilder = CohortSubsetQb,
    .cohortIds = integer(0),
    .cohortCombinationOperator = "all",
    .negate = FALSE,
    .startWindow = SubsetCohortWindow$new(),
    .endWindow = SubsetCohortWindow$new()
  ),
  public = list(
    #' to List
    #' @description List representation of object
    toList = function() {
      objRepr <- super$toList()
      objRepr$cohortIds <- private$.cohortIds
      objRepr$cohortCombinationOperator <- jsonlite::unbox(private$.cohortCombinationOperator)
      objRepr$negate <- jsonlite::unbox(private$.negate)
      objRepr$startWindow <- private$.startWindow$toList()
      objRepr$endWindow <- private$.endWindow$toList()

      objRepr
    },
    #' Get auto generated name
    #' @description name generated from subset operation properties
    #'
    #' @return character
    getAutoGeneratedName = function() {
      nameString <- ""
      if (self$negate) {
        nameString <- paste0(nameString, "not in ")
      } else {
        nameString <- paste0(nameString, "in ")
      }

      if (length(self$cohortIds) > 1) {
        nameString <- paste0(nameString, self$cohortCombinationOperator, " of ")
      }

      cohortIds <- sprintf("cohorts: (%s)", paste(self$cohortIds, collapse = ", "))
      nameString <- paste0(nameString, cohortIds)

      nameString <- paste(
        nameString,
        "starts within D:",
        self$startWindow$startDay,
        "- D:",
        self$startWindow$endDay,
        "of",
        tolower(SqlRender::camelCaseToTitleCase(self$startWindow$targetAnchor)),
        "and ends D:",
        self$endWindow$startDay,
        "- D:",
        self$endWindow$endDay,
        "of",
        tolower(SqlRender::camelCaseToTitleCase(self$endWindow$targetAnchor))
      )

      return(paste0(nameString))
    }
  ),
  active = list(
    #' @field cohortIds Integer ids of cohorts to subset to
    cohortIds = function(cohortIds) {
      if (missing(cohortIds)) {
        return(private$.cohortIds)
      }

      cohortIds <- as.integer(cohortIds)
      checkmate::assertIntegerish(cohortIds, min.len = 1)
      checkmate::assertFALSE(any(is.na(cohortIds)))
      private$.cohortIds <- cohortIds
      self
    },
    #' @field cohortCombinationOperator How to combine the cohorts
    cohortCombinationOperator = function(cohortCombinationOperator) {
      if (missing(cohortCombinationOperator)) {
        return(private$.cohortCombinationOperator)
      }

      checkmate::assertChoice(x = cohortCombinationOperator, choices = c("any", "all"))
      private$.cohortCombinationOperator <- cohortCombinationOperator
      self
    },
    #' @field negate Inverse the subset rule? TRUE will take the patients NOT in the subset
    negate = function(negate) {
      if (missing(negate)) {
        return(private$.negate)
      }

      checkmate::assertLogical(x = negate)
      private$.negate <- negate
      self
    },
    #' @field startWindow The time window to use evaluating the subset cohort
    #' start relative to the target cohort
    startWindow = function(startWindow) {
      if (missing(startWindow)) {
        return(private$.startWindow)
      }

      if (is.list(startWindow)) {
        startWindow <- do.call(createSubsetCohortWindow, startWindow)
      }

      checkmate::assertClass(x = startWindow, classes = "SubsetCohortWindow")
      private$.startWindow <- startWindow
      self
    },
    #' @field endWindow The time window to use evaluating the subset cohort
    #' end relative to the target cohort
    endWindow = function(endWindow) {
      if (missing(endWindow)) {
        return(private$.endWindow)
      }

      if (is.list(endWindow)) {
        endWindow <- do.call(createSubsetCohortWindow, endWindow)
      }

      checkmate::assertClass(x = endWindow, classes = "SubsetCohortWindow")
      private$.endWindow <- endWindow
      self
    }
  )
)

# createCohortSubset ------------------------------
#' A definition of subset functions to be applied to a set of cohorts
#' @export
#'
#' @param name  optional name of operator
#' @param cohortIds integer - set of cohort ids to subset to
#' @param cohortCombinationOperator "any" or "all" if using more than one cohort id allow a subject to be in any cohort
#'                                  or require that they are in all cohorts in specified windows
#'
#' @param startWindow               A SubsetCohortWindow that patients must fall inside (see createSubsetCohortWindow)
#' @param endWindow                 A SubsetCohortWindow that patients must fall inside (see createSubsetCohortWindow)
#' @param negate                    The opposite of this definition - include patients who do NOT meet the specified criteria (NOT YET IMPLEMENTED)
#' @returns a CohortSubsetOperator instance
createCohortSubset <- function(name = NULL, cohortIds, cohortCombinationOperator, negate, startWindow, endWindow) {
  subset <- CohortSubsetOperator$new()
  subset$name <- name
  subset$cohortIds <- cohortIds
  subset$cohortCombinationOperator <- cohortCombinationOperator
  subset$negate <- negate
  subset$startWindow <- startWindow
  subset$endWindow <- endWindow

  subset
}

# DemographicSubsetOperator ------------------------------
#' Criteria Subset
#' @export
DemographicSubsetOperator <- R6::R6Class(
  classname = "DemographicSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    queryBuilder = DemographicSubsetQb,
    suffixStr = "Demo",
    .ageMin = 0,
    .ageMax = 99999,
    .gender = NULL,
    .race = NULL,
    .ethnicity = NULL
  ),
  public = list(
    #' @description List representation of object
    toList = function() {
      objRepr <- super$toList()
      if (length(private$.ageMin)) {
        objRepr$ageMin <- jsonlite::unbox(private$.ageMin)
      }
      if (length(private$.ageMax)) {
        objRepr$ageMax <- jsonlite::unbox(private$.ageMax)
      }
      if (!is.null(private$.gender)) {
        objRepr$gender <- private$.gender
      }
      if (!is.null(private$.race)) {
        objRepr$race <- private$.race
      }
      if (!is.null(private$.ethnicity)) {
        objRepr$ethnicity <- private$.ethnicity
      }

      objRepr
    },
    #' Map gender concepts to names
    #' @param mapping               optional list of mappings for concept id to nouns
    #' @returns char vector
    mapGenderConceptsToNames = function(mapping = list(
                                          "8507" = "males",
                                          "8532" = "females",
                                          "0" = "unknown gender"
                                        )) {
      conceptMap <- c()
      if (length(self$gender)) {
        for (x in self$gender) {
          mp <- ifelse(!is.null(mapping[[as.character(x)]]), mapping[[as.character(x)]], paste("gender concept:", x))
          if (is.null(mp)) {
            mp <- x
          }
          conceptMap <- c(conceptMap, mp)
        }
      }

      return(conceptMap)
    },

    #' Get auto generated name
    #' @description name generated from subset operation properties
    #'
    #' @return character
    getAutoGeneratedName = function() {
      nameString <- ""

      if (!is.null(self$gender)) {
        nameString <- paste0(nameString, "", paste(self$mapGenderConceptsToNames(), collapse = ", "))
      }

      if (length(self$ageMin) && self$ageMin > 0) {
        if (nameString != "") {
          nameString <- paste0(nameString, " ")
        }

        nameString <- paste0(nameString, "aged ", self$ageMin)

        if (length(self$ageMax) && self$ageMax < 99999) {
          nameString <- paste0(nameString, " - ")
        } else {
          nameString <- paste0(nameString, "+")
        }
      }

      if (length(self$ageMax) && self$ageMax < 99999) {
        if (length(self$ageMin) && self$ageMin == 0) {
          if (nameString != "") {
            nameString <- paste0(nameString, " ")
          }

          nameString <- paste0(nameString, "aged <=")
        }
        nameString <- paste0(nameString, self$ageMax)
      }

      if (!is.null(private$.race)) {
        if (nameString != "") {
          nameString <- paste0(nameString, ", ")
        }

        nameString <- paste0(nameString, "race: ", paste(self$race, collapse = ", "))
      }
      if (!is.null(private$.ethnicity)) {
        if (nameString != "") {
          nameString <- paste0(nameString, ", ")
        }

        nameString <- paste0(nameString, "ethnicity: ", paste(self$ethnicity, collapse = ", "))
      }

      return(nameString)
    },

    #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },


    #' @description Compare Subset to another
    #' @param criteria DemographicSubsetOperator instance
    isEqualTo = function(criteria) {
      checkmate::assertR6(criteria, "DemographicSubsetOperator")
      return(all(
        self$ageMin == criteria$ageMin,
        self$ageMax == criteria$ageMax,
        self$getGender() == criteria$getGender(),
        self$getRace() == criteria$getRace(),
        self$getEthnicity() == criteria$getEthnicity()
      ))
    },


    #' @description Gender getter - used when constructing SQL to default
    #' NULL to an empty string
    getGender = function() {
      if (is.null(private$.gender)) {
        return("")
      } else {
        return(private$.gender)
      }
    },

    #' @description Race getter - used when constructing SQL to default
    #' NULL to an empty string
    getRace = function() {
      if (is.null(private$.race)) {
        return("")
      } else {
        return(private$.race)
      }
    },

    #' @description Ethnicity getter - used when constructing SQL to default
    #' NULL to an empty string
    getEthnicity = function() {
      if (is.null(private$.ethnicity)) {
        return("")
      } else {
        return(private$.ethnicity)
      }
    }
  ),
  active = list(
    #' @field    ageMin Int between 0 and 99999 - minimum age
    ageMin = function(ageMin) {
      if (missing(ageMin)) {
        return(private$.ageMin)
      }
      checkmate::assertInt(ageMin, lower = 0, upper = min(self$ageMax, 99999))
      private$.ageMin <- ageMin
      return(self)
    },
    #' @field  ageMax  Int between 0 and 99999 - maximum age
    ageMax = function(ageMax) {
      if (missing(ageMax)) {
        return(private$.ageMax)
      }
      checkmate::assertInt(ageMax, lower = max(0, self$ageMin), upper = 99999)
      private$.ageMax <- ageMax
      return(self)
    },
    #' @field gender vector of gender concept IDs
    gender = function(gender) {
      if (missing(gender)) {
        return(private$.gender)
      }
      checkmate::assertVector(gender, null.ok = TRUE)
      private$.gender <- gender
      return(self)
    },
    #' @field race character string denoting race
    race = function(race) {
      if (missing(race)) {
        return(private$.race)
      }
      checkmate::assertVector(race, null.ok = TRUE)
      private$.race <- race
      return(self)
    },
    #' @field ethnicity character string denoting ethnicity
    ethnicity = function(ethnicity) {
      if (missing(ethnicity)) {
        return(private$.ethnicity)
      }
      checkmate::assertVector(ethnicity, null.ok = TRUE)
      private$.ethnicity <- ethnicity
      return(self)
    }
  )
)

# createDemographicSubset ------------------------------
#' Create createDemographicSubset Subset
#' @export
#' @param name         Optional char name
#' @param ageMin       The minimum age
#' @param ageMax       The maximum age
#' @param gender       Gender demographics - concepts - 0, 8532, 8507, 0 "male", "female".
#'                     Any string that is not (case insensitive) "male" or "female" is converted to gender concept 0
#'                     https://www.ohdsi.org/web/wiki/doku.php?id=documentation:vocabulary:gender
#'                     Specific concept ids not in this set can be used but are not explicitly validated
#' @param race         Race demographics - concept ID list
#' @param ethnicity    Ethnicity demographics - concept ID list
createDemographicSubset <- function(name = NULL, ageMin = 0, ageMax = 99999, gender = NULL, race = NULL, ethnicity = NULL) {
  mapGenderCodes <- function(x) {
    if (length(x) > 1) {
      retValue <- c()
      for (i in x) {
        retValue <- c(retValue, mapGenderCodes(i))
      }
      return(retValue)
    }
    if (is.character(x)) {
      x <- tolower(x)
      if (x == "male") {
        return(8507)
      } else if (x == "female") {
        return(8532)
      }
      return(0)
    }
    return(x)
  }

  if (!is.null(gender)) {
    gender <- mapGenderCodes(gender)
  }

  subset <- DemographicSubsetOperator$new()
  subset$name <- name
  subset$ageMin <- ageMin
  subset$ageMax <- ageMax
  subset$gender <- gender
  subset$race <- race
  subset$ethnicity <- ethnicity

  subset
}


# LimitSubsetOperator ------------------------------
#' @title Limit Subset Operator
#' @export
#' @description operator to apply limiting subset operations (e.g. washout periods, calendar ranges or earliest entries)
#'
LimitSubsetOperator <- R6::R6Class(
  classname = "LimitSubsetOperator",
  inherit = SubsetOperator,
  private = list(
    queryBuilder = LimitSubsetQb,
    suffixStr = "Limit to",
    .priorTime = 0,
    .followUpTime = 0,
    .limitTo = character(0),
    .calendarStartDate = NULL,
    .calendarEndDate = NULL
  ),
  public = list(
    #' Get auto generated name
    #' @description name generated from subset operation properties
    #'
    #' @return character
    getAutoGeneratedName = function() {
      nameString <- ""

      if (self$limitTo != "all") {
        nameString <- paste0(nameString, tolower(SqlRender::camelCaseToTitleCase(self$limitTo)), " occurence")
      } else {
        nameString <- paste0(nameString, "occurs")
      }

      if (self$priorTime > 0) {
        nameString <- paste0(nameString, " with at least ", self$priorTime, " days prior observation")
      }

      if (self$followUpTime > 0) {
        if (self$priorTime > 0) {
          nameString <- paste(nameString, "and")
        } else {
          nameString <- paste(nameString, "with at least")
        }
        nameString <- paste(nameString, self$followUpTime, "days follow up observation")
      }

      if (!is.null(self$calendarStartDate)) {
        nameString <- paste(nameString, "after", self$calendarStartDate)
      }

      if (!is.null(self$calendarEndDate)) {
        if (!is.null(self$calendarStartDate)) {
          nameString <- paste(nameString, "and")
        }
        nameString <- paste(nameString, "before", self$calendarEndDate)
      }

      return(nameString)
    },
    #' To List
    #' @description List representation of object
    toList = function() {
      objRef <- super$toList()
      objRef$priorTime <- jsonlite::unbox(private$.priorTime)
      objRef$followUpTime <- jsonlite::unbox(private$.followUpTime)
      objRef$limitTo <- jsonlite::unbox(private$.limitTo)
      objRef$calendarStartDate <- jsonlite::unbox(private$.calendarStartDate)
      objRef$calendarEndDate <- jsonlite::unbox(private$.calendarEndDate)

      objRef
    }
  ),
  active = list(
    #' @field priorTime             minimum washout time in days
    priorTime = function(priorTime) {
      if (missing(priorTime)) {
        return(private$.priorTime)
      }
      checkmate::assertInt(priorTime, lower = 0, upper = 99999)
      private$.priorTime <- priorTime
      self
    },
    #' @field followUpTime            minimum required follow up time in days
    followUpTime = function(followUpTime) {
      if (missing(followUpTime)) {
        return(private$.followUpTime)
      }

      checkmate::assertInt(followUpTime, lower = 0, upper = 99999)
      private$.followUpTime <- followUpTime
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
      if (missing(limitTo)) {
        return(private$.limitTo)
      }

      checkmate::assertCharacter(limitTo)

      # maintain support for old versions
      if (limitTo == "") {
        limitTo <- "all"
      }

      checkmate::assertChoice(limitTo, choices = c("all", "firstEver", "earliestRemaining", "latestRemaining", "lastEver"))
      private$.limitTo <- limitTo
      self
    },
    #' @field calendarStartDate            The calendar start date for limiting by date
    calendarStartDate = function(calendarStartDate) {
      if (missing(calendarStartDate)) {
        return(private$.calendarStartDate)
      }

      if (is.character(calendarStartDate)) {
        if (calendarStartDate == "") {
          calendarStartDate <- NULL
        } else {
          calendarStartDate <- lubridate::date(calendarStartDate)
        }
      }
      checkmate::assertDate(calendarStartDate, null.ok = TRUE)
      private$.calendarStartDate <- calendarStartDate
      self
    },
    #' @field calendarEndDate            The calendar end date for limiting by date
    calendarEndDate = function(calendarEndDate) {
      if (missing(calendarEndDate)) {
        return(private$.calendarEndDate)
      }

      if (is.character(calendarEndDate)) {
        if (calendarEndDate == "") {
          calendarEndDate <- NULL
        } else {
          calendarEndDate <- lubridate::date(calendarEndDate)
        }
      }
      checkmate::assertDate(calendarEndDate, null.ok = TRUE)
      private$.calendarEndDate <- calendarEndDate
      self
    }
  )
)

# createLimitSubset ------------------------------
#' Create Limit Subset
#' @description
#' Subset cohorts using specified limit criteria
#' @export
#' @param name              Name of operation
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
#' @param calendarEndDate       Start date to allow period (e.g. 2015/1/1)
#' @param calendarStartDate     End date to allow periods (e.g. 2020/1/1/)
createLimitSubset <- function(name = NULL,
                              priorTime = 0,
                              followUpTime = 0,
                              limitTo = "all",
                              calendarStartDate = NULL,
                              calendarEndDate = NULL) {
  if (limitTo == "" || is.null(limitTo)) {
    limitTo <- "all"
  }

  if (priorTime == 0 & followUpTime == 0 & limitTo == "all" & is.null(calendarStartDate) & is.null(calendarEndDate)) {
    stop("No limit criteria specified")
  }

  subset <- LimitSubsetOperator$new()
  subset$name <- name
  subset$priorTime <- priorTime
  subset$followUpTime <- followUpTime
  subset$limitTo <- limitTo
  subset$calendarStartDate <- calendarStartDate
  subset$calendarEndDate <- calendarEndDate

  subset
}
