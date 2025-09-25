# Copyright 2025 Observational Health Data Sciences and Informatics
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


#' Add Indication Subset Definition
#' @export
#' @description
#' Utility pattern for creating an indication subset from a set of target cohorts.
#' The approach applies this definition to an exposure or set of exposures, requiring individuals to have
#' a prior history of a condition before receiving treatment. By default, this creats new cohorts that have evidence
#' of the indication cohort at any point in their history.
#' For many situations it may be preffered to look back within a specific window of drug exposure and the look window
#' period should be accordingly.
#' Additionally, the R attribute of "indicationSubsetDefinitions" is attached to the cohort definition set.
#' This can be obtained by calling `getIndicationSubsetDefinitionIds`, which should return the set of subset definition
#' ids that are associated with indications.
#'
#' @examples
#' \dontrun{
#' library(CohortGenerator)
#'
#' initialSet <- getCohortDefinitionSet(
#'   settingsFileName = "testdata/name/Cohorts.csv",
#'   jsonFolder = "testdata/name/cohorts",
#'   sqlFolder = "testdata/name/sql/sql_server",
#'   cohortFileNameFormat = "%s",
#'   cohortFileNameValue = c("cohortName"),
#'   packageName = "CohortGenerator",
#'   verbose = FALSE
#' )
#'
#' print(initialSet[, c("cohortId", "cohortName")])
#'
#' # Subset cohorts 1 & 2 by an "indication" cohort 3:
#' res <- addIndicationSubsetDefinition(
#'   cohortDefinitionSet = initialSet,
#'   targetCohortIds = c(1, 2),
#'   indicationCohortIds = c(3),
#'   subsetDefinitionId = 10
#' )
#'
#' print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])
#'
#' # Get all subset definitions that were created using the addIndicationSubsetDefinition:
#' subsetDefinitionId <- getIndicationSubsetDefinitionIds(res)
#'
#' # Filter the cohortDefinitionSet to those cohorts defined using an indication subset definition:
#' newCohorts <- res |>
#'   dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
#'   dplyr::select(cohortId, cohortName, subsetParent, isSubset)
#' print(newCohorts)
#' }
#'
#' @template cohortDefinitionSet
#' @param targetCohortIds               Set of integer cohort IDs. Must be within the cohort definition set.
#' @param indicationCohortIds           Set of integer cohort IDs. Must be within the cohort definition set.
#' @param subsetDefinitionId            Unique integer Id of the subset definition
#' @param subsetDefinitionName          name of the subset definition (used in resulting cohort definitions)
#' @param subsetCohortNameTemplate      template string format for naming resulting cohorts
#' @param cohortCombinationOperator     Logic for multiple indication cohort IDs: any (default) or all.
#' @param lookbackWindowStart           Start of lookback period.
#' @param lookbackWindowEnd             End of lookback period.
#' @param lookForwardWindowStart        When the indication can end relative to index; default is 0.
#' @param lookForwardWindowEnd          When the indication can end relative to index; default is 9999.
#' @param studyStartDate                Exclude patients with index prior to this date (format "\%Y\%m\%d").
#' @param studyEndDate                  Exclude patients with index after this date (format "\%Y\%m\%d").
#' @param genderConceptIds              Gender concepts to require
#' @param ageMin                        Minimum age at target index.
#' @param ageMax                        Maximum age at target index.
#' @param requiredPriorObservationTime  Observation time prior to index; default 365.
#' @param requiredFollowUpTime          Observation time after index; default 1.
addIndicationSubsetDefinition <- function(cohortDefinitionSet,
                                          targetCohortIds,
                                          indicationCohortIds,
                                          subsetDefinitionId,
                                          subsetDefinitionName,
                                          subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
                                          cohortCombinationOperator = "any",
                                          lookbackWindowStart = -99999,
                                          lookbackWindowEnd = 0,
                                          lookForwardWindowStart = 0,
                                          lookForwardWindowEnd = 99999,
                                          genderConceptIds = NULL,
                                          ageMin = NULL,
                                          ageMax = NULL,
                                          studyStartDate = NULL,
                                          studyEndDate = NULL,
                                          requiredPriorObservationTime = 365,
                                          requiredFollowUpTime = 1) {

  .cohortDefinitionSetHasRequiredColumns(cohortDefinitionSet)
  checkmate::assertTRUE(all(targetCohortIds %in% cohortDefinitionSet$cohortId))
  checkmate::assertTRUE(all(indicationCohortIds %in% cohortDefinitionSet$cohortId))

  subsetOperators <- list()
  subsetOperators[[length(subsetOperators) + 1]] <- createCohortSubset(
    cohortIds = indicationCohortIds,
    negate = FALSE,
    cohortCombinationOperator = cohortCombinationOperator,
    windows = list(
      createSubsetCohortWindow(lookbackWindowStart, lookbackWindowEnd, "cohortStart"),
      createSubsetCohortWindow(lookForwardWindowStart, lookForwardWindowEnd, "cohortStart")
    )
  )

  subsetOperators[[length(subsetOperators) + 1]] <- createLimitSubset(
    priorTime = requiredPriorObservationTime,
    followUpTime = requiredFollowUpTime,
    limitTo = "firstEver",
    calendarStartDate = as.Date(studyStartDate, "%Y%m%d"),
    calendarEndDate = as.Date(studyEndDate, "%Y%m%d")
  )

  if (any(!is.null(c(genderConceptIds, ageMin, ageMax)))) {
    subsetOperators[[length(subsetOperators) + 1]] <- createDemographicSubset(
      ageMin = ifelse(is.null(ageMin), 0, ageMin),
      ageMax = ifelse(is.null(ageMax), 99999, ageMax),
      gender = genderConceptIds
    )
  }

  subsetDef <- createCohortSubsetDefinition(
    name = subsetDefinitionName,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    definitionId = subsetDefinitionId,
    subsetOperators = subsetOperators
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = subsetDef,
      targetCohortIds = targetCohortIds
    )


  attr(cohortDefinitionSet, "indicationSubsetDefinitions") <- c(getIndicationSubsetDefinitionIds(cohortDefinitionSet),
                                                                subsetDefinitionId)
  return(cohortDefinitionSet)
}


#' Add Restriction Subset Definition
#' @export
#' @description
#' Utility pattern for creating cohort subset definitions as a standard approach for indicated drugs.
#' Restriction subset definitions are twins of indication definitions. They should apply the same core properites
#' to a base exposure cohort (i.e. study dates,  required prior observation time, ages, gender) as indications but,
#' crucially, they do not require history of any prior condition(s).
#'
#' This is useful in the context of comparing drug exposure + indication population, to population as a whole.
#'
#' The prefered use of this function is to create this in conjunction with the target population.
#' @inheritParams addIndicationSubsetDefinition
#' @examples
#' \dontrun{
#' library(CohortGenerator)
#'
#' initialSet <- getCohortDefinitionSet(
#'   settingsFileName = "testdata/name/Cohorts.csv",
#'   jsonFolder = "testdata/name/cohorts",
#'   sqlFolder = "testdata/name/sql/sql_server",
#'   cohortFileNameFormat = "%s",
#'   cohortFileNameValue = c("cohortName"),
#'   packageName = "CohortGenerator",
#'   verbose = FALSE
#' )
#'
#' print(initialSet[, c("cohortId", "cohortName")])
#'
#' # Restrinct to first occurrence of cohort
#' res <- addRestrictionSubsetDefinition(
#'   cohortDefinitionSet = initialSet,
#'   targetCohortIds = c(1, 2),
#'   subsetDefinitionId = 20
#' )
#'
#' print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])
#'
#' # Get all subset definitions that were created using the addRestrictionSubsetDefinition:
#' subsetDefinitionId <- getRestrictionSubsetDefinitionIds(res)
#'
#' # Filter the cohortDefinitionSet to those cohorts defined using an restriction subset definition:
#' newCohorts <- res |>
#'   dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
#'   dplyr::select(cohortId, cohortName, subsetParent, isSubset)
#' print(newCohorts)
#' }
addRestrictionSubsetDefinition <- function(cohortDefinitionSet,
                                           targetCohortIds,
                                           subsetDefinitionId,
                                           subsetDefinitionName,
                                           subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
                                           genderConceptIds = NULL,
                                           ageMin = NULL,
                                           ageMax = NULL,
                                           studyStartDate = NULL,
                                           studyEndDate = NULL,
                                           requiredPriorObservationTime = 365,
                                           requiredFollowUpTime = 1) {

  .cohortDefinitionSetHasRequiredColumns(cohortDefinitionSet)
  checkmate::assertChoice(targetCohortIds, cohortDefinitionSet$cohortId)

  subsetOperators <- list()
  subsetOperators[[length(subsetOperators) + 1]] <- createLimitSubset(
    priorTime = requiredPriorObservationTime,
    followUpTime = requiredFollowUpTime,
    limitTo = "firstEver",
    calendarStartDate = as.Date(studyStartDate, "%Y%m%d"),
    calendarEndDate = as.Date(studyEndDate, "%Y%m%d")
  )

  if (any(!is.null(c(genderConceptIds, ageMin, ageMax)))) {
    subsetOperators[[length(subsetOperators) + 1]] <- createDemographicSubset(
      ageMin = ifelse(is.null(ageMin), 0, ageMin),
      ageMax = ifelse(is.null(ageMax), 99999, ageMax),
      gender = genderConceptIds
    )
  }

  subsetDef <- createCohortSubsetDefinition(
    name = subsetDefinitionName,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    definitionId = subsetDefinitionId,
    subsetOperators = subsetOperators
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = subsetDef,
      targetCohortIds = targetCohortIds
    )

  attr(cohortDefinitionSet, "restrictionSubsetDefinitions") <- c(getRestrictionSubsetDefinitionIds(cohortDefinitionSet),
                                                                 subsetDefinitionId)
  return(cohortDefinitionSet)
}

#' Add exclude on index subset definition
#' @description
#' The purpose of this subset recipe is to exclude all individuals if their index aligns with the specified
#' exclusion cohort ids.
#' If the index date of the exclusionCohortIds aligns with the targetCohortIds (or it lies within some relative window
#' of the target cohort start date) then they will be excluded from the resulting sub population.
#'
#' This may be used in situations where an outcome cohort may contain individuals treated for a target medication,
#' complicating calculation of incidence rates.
#'
#' @export
#'
#' @inheritParams addIndicationSubsetDefinition
#' @param exclusionCohortIds                        cohort ids to exclude members of target from
#' @param exclusionWindow                           Days Default is 0 (target index date). by changing this
#'                                                  you can adjust the period around target index for which you would
#'                                                  exclude members.
#'
#' @examples
#' \dontrun{
#' library(CohortGenerator)
#'
#' initialSet <- getCohortDefinitionSet(
#'   settingsFileName = "testdata/name/Cohorts.csv",
#'   jsonFolder = "testdata/name/cohorts",
#'   sqlFolder = "testdata/name/sql/sql_server",
#'   cohortFileNameFormat = "%s",
#'   cohortFileNameValue = c("cohortName"),
#'   packageName = "CohortGenerator",
#'   verbose = FALSE
#' )
#'
#' print(initialSet[, c("cohortId", "cohortName")])
#'
#' # Subset cohorts 1 & 2 by an "indication" cohort 3:
#' res <- addExcludeOnIndexSubsetDefinition(
#'   cohortDefinitionSet = initialSet,
#'   targetCohortIds = c(1, 2),
#'   exclusionCohortIds = c(3),
#'   subsetDefinitionId = 20,
#'   subsetDefinitioName = 'Exclude on index if in cohort 3'
#' )
#'
#' print(res[, c("cohortId", "cohortName", "subsetParent", "subsetDefinitionId", "isSubset")])
#'
#' # Get all subset definitions that were created using the addExcludeOnIndexSubsetDefinition:
#' subsetDefinitionId <- getExcludeOnIndexSubsetDefinitionIds(res)
#'
#' # Filter the cohortDefinitionSet to those cohorts defined using an exclusion subset definition:
#' newCohorts <- res |>
#'   dplyr::filter(subsetDefinitionId == subsetDefinitionId) |>
#'   dplyr::select(cohortId, cohortName, subsetParent, isSubset)
#' print(newCohorts)
#' }
addExcludeOnIndexSubsetDefinition <- function(cohortDefinitionSet,
                                              subsetDefinitionName,
                                              subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
                                              targetCohortIds,
                                              exclusionCohortIds,
                                              exclusionWindow = 0,
                                              subsetDefinitionId,
                                              cohortCombinationOperator = "any") {
  .cohortDefinitionSetHasRequiredColumns(cohortDefinitionSet)
  checkmate::assertTRUE(all(targetCohortIds %in% cohortDefinitionSet$cohortId))
  checkmate::assertTRUE(all(exclusionCohortIds %in% cohortDefinitionSet$cohortId))

  op <- CohortGenerator::createCohortSubset(
    cohortIds = exclusionCohortIds,
    name = "exclusion",
    negate = TRUE, # LOGIC -  NOT IN  any indication cohort on cohort start date
    cohortCombinationOperator = cohortCombinationOperator,
    windows = list(CohortGenerator::createSubsetCohortWindow(exclusionWindow, exclusionWindow, "cohortStart"))
  )

  def <- CohortGenerator::createCohortSubsetDefinition(
    name = subsetDefinitionName,
    definitionId = subsetDefinitionId,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    subsetOperators = list(op)
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = def,
      targetCohortIds = targetCohortIds
    )

  attr(cohortDefinitionSet, "excludeOnIndexSubsetDefinitions") <- c(getExcludeOnIndexSubsetDefinitionIds(cohortDefinitionSet),
                                                                    subsetDefinitionId)

  return(cohortDefinitionSet)
}

#' Get Indication Subset Definition Ids
#' @description
#' Get the indication subset definition ids from a cohort definition set (if any have been added)
#' Useful if keeping track in a script with complex business logic around what a cohort definition is for
#' @export
#' @template cohortDefinitionSet
getIndicationSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "indicationSubsetDefinitions", exact = TRUE)
}


#' Get Restriction Subset Definition Ids
#' @description
#' Get the restriction subset definition ids from a cohort definition set (if any have been added)
#' Useful if keeping track in a script with complex business logic around what a cohort definition is for
#'
#' @export
#' @template cohortDefinitionSet
getRestrictionSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "restrictionSubsetDefinitions", exact = TRUE)
}


#' Get Exclude On Index Subset Definition Ids
#' @description
#' Get the exclusion on index subset definition ids from a cohort definition set (if any have been added)
#' Useful if keeping track in a script with complex business logic around what a cohort definition is for
#' @export
#' @template cohortDefinitionSet
getExcludeOnIndexSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "excludeOnIndexSubsetDefinitions", exact = TRUE)
}
