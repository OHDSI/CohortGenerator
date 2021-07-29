# Copyright 2021 Observational Health Data Sciences and Informatics
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

#' Create the Circe cohort expression from a JSON file
#'
#' @description
#' This function constructs a Circe cohort expression from a JSON file for use
#' with other CirceR functions.
#'
#' @param filePath      The file path containing the Circe JSON file
#'
#' @export
createCirceExpressionFromFile <- function(filePath) {
  cohortExpression <- readCirceExpressionJsonFile(filePath)
  return(CirceR::cohortExpressionFromJson(cohortExpression))
}

#' Read a JSON file for use with CirceR
#'
#' @description
#' This function wraps the default readlines call for calling CirceR.
#'
#' @param filePath      The file path containing the Circe JSON file
#'
#' @export
readCirceExpressionJsonFile <- function(filePath) {
  return(paste(readLines(filePath, warn = FALSE), collapse = "\n"))
}
