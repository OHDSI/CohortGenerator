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
#' @description
#' Utility pattern for creating cohort subset definitions as a standaridzed approach for indicated drugs.
#' The general approach is to apply this definition to an exposure or set of exposures such that an individual must
#' show prior history of some condition prior to recieving treatment.
#'
#' This is a function designed to make parameterization of study execution clear.
#'
#' Also attaches an attribute to the cohort definition set
#' @export
#' @template cohortDefinitionSet
#' @param subsetDefinitionId            The ID if the resulting subset. Note, this must be uniquely applied to the cohort
#'                                      definition set.
#' @param indicationCohortIds           set of intger cohort ids. Must be within the cohort definition set
#' @param cohortCombinationOperator     If there is more than one cohort id as an indication condition, what logic should
#'                                      be required for inclusion - default is any. Alternatively, all prior diseases
#'                                      can be required in the specified window.
#'                                      Specifying 'all' can be useful in the case of severe conditions (e.g. individuals with
#'                                      prior disease that also require prior exposure to a previous drug for which the
#'                                      targetExposureId is a second line treatment)
#' @param lookbackWindowStart           start of the lookback period for valid inclusion within. How long prior can
#'                                      an individual have a condition
#' @param lookbackWindowEnd             end of the lookback period for valid inclusion within. This is normally day 0,
#'                                      index. However, there may be situations where there must be a gap between diagnosis
#'                                      and indication start.
#' @param lookForwardWindowStart        When can the indicated condition end relative to the target Default - day 0
#' @param lookForwardWindowEnd          When can the indicated condition end relative to the target Default 9999
#' @param studyStartDate                Exclude patients index prior to this date. Must be in date format - "%Y%m%d" (e.g. 2001/12/25)
#' @param studyEndDate                  Exclude patients with index after this date. Must be in date format - "%Y%m%d" (e.g. 2001/12/25)
#' @param ageMin                        Does this population require a minimum age (at target index)?
#' @param ageMax                        Does this population require a maximum age (at target index)?
#' @param requiredPriorObservationTime  Observation time in data source required prior to index (a.k.a washout perioid)
#'                                      Deafults to 365 days.
#' @param requiredFollowUpTime          Observation time after target index required (typically at least 1 day, default)
#'
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
#' @export
#' @template cohortDefinitionSet
getRestrictionSubsetDefinitionIds <- function(cohortDefinitionSet) {
  attr(cohortDefinitionSet, "indicationSubsetDefinitions", exact = TRUE)
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