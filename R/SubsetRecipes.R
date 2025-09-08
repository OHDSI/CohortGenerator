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


#' Get Indication Subset Definition Ids
#' @description
#' Get the indication subset definition ids from a cohort definition set (if any have been added)
#' Useful if keeping track in a script with complex business logic around what a cohort definition is for
#' @export
#' @template cohortDefinitionSet
getIndicationSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "indicationSubsetDefinitions", exact = TRUE)
}

#' Add Indication Subset Definition
#'
#' @description
#' Utility pattern for creating cohort subset definitions, following a standard approach for indicating drugs.
#' The approach applies this definition to an exposure or set of exposures, requiring individuals to have
#' a prior history of a condition before receiving treatment.
#' This function aims to make parameterization of study execution explicit.
#' Additionally, it attaches an attribute to the cohort definition set.
#'
#' @param subsetDefinitionId            The ID for the resulting subset. Must be unique within the cohort definition set.
#' @param indicationCohortIds           Set of integer cohort IDs. Must be within the cohort definition set.
#' @param cohortCombinationOperator     Defines the logic for multiple indication cohort IDs:
#'                                      - `'any'` (default): individual must meet any indication.
#'                                      - `'all'`: individual must have all indications within the specified window.
#'                                      Useful for severe conditions requiring multiple prior diagnoses or treatments.
#' @param lookbackWindowStart           Start of the lookback period for valid inclusion. Measures how long prior to index an individual can have a condition.
#' @param lookbackWindowEnd             End of the lookback period. Typically day 0 (index), but can specify a gap if needed.
#' @param lookForwardWindowStart        The earliest time relative to the target index when the indication condition can end. Default is day 0.
#' @param lookForwardWindowEnd          The latest time relative to the target index when the indication condition can end. Default is 9999.
#' @param studyStartDate                Exclude patients with index prior to this date. Format: "%Y%m%d" (e.g., 20011225).
#' @param studyEndDate                  Exclude patients with index after this date. Format: "%Y%m%d" (e.g., 20011225).
#' @param ageMin                        Minimum age at target index required for inclusion.
#' @param ageMax                        Maximum age at target index required for inclusion.
#' @param requiredPriorObservationTime  Observation time needed prior to index (washing out period). Defaults to 365 days.
#' @param requiredFollowUpTime          Observation time after index, typically at least 1 day.
#' @template cohortDefinitionSet
#' @export
addIndicationSubsetDefinition <- function(cohortDefinitionSet,
                                          targetCohortIds,
                                          indicationCohortIds,
                                          definitionId,
                                          name = "study population + first",
                                          subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
                                          cohortCombinationOperator = "any",
                                          lookbackWindowStart = -99999,
                                          loockbackWindowEnd = 0,
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
  checkmate::assertChoice(targetCohortIds, cohortDefinitionSet$cohortId)
  checkmate::assertChoice(indicationCohortIds, cohortDefinitionSet$cohortId)

  subsetOperators <- list()
  subsetOperators[[length(subsetOperators) + 1]] <- createCohortSubset(
    cohortIds = uniqueSubsetCriteria$indicationId,
    negate = FALSE,
    cohortCombinationOperator = cohortCombinationOperator,
    windows = list(
      createSubsetCohortWindow(lookbackWindowStart, loockbackWindowEnd, "cohortStart"),
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
      ageMin = ageMin,
      ageMax = ageMax,
      gender = genderConceptIds
    )
  }

  subsetDef <- createCohortSubsetDefinition(
    name = name,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    definitionId = definitionId,
    subsetOperators = subsetOperators
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = subsetDef,
      targetCohortIds = targetCohortIds
    )


  attr(cohortDefinitionSet, "indicationSubsetDefinitions") <- c(getIndicationSubsetDefinitionIds(cohortDefinitionSet),
                                                                definitionId)
  return(cohortDefinitionSet)
}

#' Get Indication Subset Definition Ids
#' @description
#' Get the indication subset definition ids from a cohort definition set (if any have been added)
#' Useful if keeping track in a script with complex business logic around what a cohort definition is for
#'
#' @export
#' @template cohortDefinitionSet
getRestrictionSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "restrictionSubsetDefinitions", exact = TRUE)
}

#' Add Restriction Subset Definition
#' @description
#' Utility pattern for creating cohort subset definitions as a standaridzed approach for indicated drugs.
#' Restriction subset definitions are twins of indication definitions. They should apply the same core properites
#' to a base exposure cohort (i.e. study dates,  required prior observation time, ages, gender) as indications but,
#' crucially, they do not require history of any prior conidtion(s).
#'
#' This is useful in the context of comparing drug exposure + indication population, to population as a whole.
#'
#' The prefered use of this function is to create this in conjunction with the target population.
#'
#' @export
#' @inheritParams addIndicationSubsetDefinition
addRestrictionSubsetDefinition <- function(cohortDefinitionSet,
                                          targetCohortIds,
                                          definitionId,
                                          name = "study population + first",
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
      ageMin = ageMin,
      ageMax = ageMax,
      gender = genderConceptIds
    )
  }

  subsetDef <- createCohortSubsetDefinition(
    name = name,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    definitionId = definitionId,
    subsetOperators = subsetOperators
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = subsetDef,
      targetCohortIds = targetCohortIds
    )

  attr(cohortDefinitionSet, "restrictionSubsetDefinitions") <- c(getRestrictionSubsetDefinitionIds(cohortDefinitionSet),
                                                                definitionId)
  return(cohortDefinitionSet)
}

#' Add exclude on index subset definition
#' @description
#' The purpose of this subset recipie is to exclude all individuals if their index aligns with the specified
#' exclusion cohort ids.This may be used in situations where an outcome cohort may contain individuals treated for a target medication,
#' complicating calculation of incidence rates.
#'
#' @inheritParams addIndicationSubsetDefinition
#' @param exclusionCohortIds                        cohort ids to exlcude members of target from
#' @param cohortCombinationOperator                 if more than one cohort is used, combine them all with any or only
#'                                                  exclude if they're in a single cohort definition
#' @param exclusionWindow                           Days Default is 0 (target index date). by changing this
#'                                                  you can adjust the period around target index for which you would
#'                                                  exclude members.
#' @export
addExcludeOnIndexSubsetDefinition <- function(cohortDefinitionSet,
                                              name,
                                              subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName",
                                              targetCohortIds,
                                              exclusionCohortIds,
                                              exclusionWindow = 0,
                                              definitionId,
                                              cohortCombinationOperator = "any") {
  .cohortDefinitionSetHasRequiredColumns(cohortDefinitionSet)
  checkmate::assertChoice(targetCohortIds, cohortDefinitionSet$cohortId)
  checkmate::assertChoice(exclusionCohortIds, cohortDefinitionSet$cohortId)

  def <- CohortGenerator::createCohortSubset(
    cohortIds = exclusionCohortIds,
    name = "exclusion",
    negate = TRUE, # LOGIC -  NOT IN  any indication cohort on cohort start date
    cohortCombinationOperator = cohortCombinationOperator,
    windows = list(CohortGenerator::createSubsetCohortWindow(exclusionWindow, exclusionWindow, "cohortStart"))
  )

  CohortGenerator::createCohortSubsetDefinition(
    name = name,
    definitionId = definitionId,
    subsetCohortNameTemplate = subsetCohortNameTemplate,
    subsetOperators = list(def)
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(
      cohortSubsetDefintion = def,
      targetCohortIds = targetCohortIds
    )

  return(cohortDefinitionSet)
}
