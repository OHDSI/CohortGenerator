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

#' Query Builder
#' @description
#' Base class for all subset operator query builders.
#' These turn subset class representations into query builder instances that can be used to construct fully formed
#' SQL queries
#'
#' This base class is intented to be used in an abstract way. All subset operators should map to the functions within
#' Note, that a query may have no modifications to the join, logic or having clauses, in which case no implementation is
#' required - NULL is the default representation
QueryBuilder <- R6::R6Class(
  classname = "QueryBuilder",
  private = list(
    operator = NULL
  ),
  public = list(
    initialize = function(operator) {
      checkmate::assertR6(operator, "SubsetOperator")
      private$operator <- operator
    },
    #' Get Join Statments for subsetting query
    getJoinStatments = function() {
      NULL
    },

                #' Get Having Cluases Statments for subsetting query
    getHavingClauses = function() {
      NULL
    },

                #' Get Logic Statments for subsetting query
    getLogic = function() {
      NULL
    },

    getQueryParts = function() {
      return(list(
        logic = self$getLogic(),
        havingClauses = self$getHavingClauses(),
        joins = self$getJoinStatments()
      ))
    }
  )
)

CohortSubsetQb <- R6::R6Class(
  classname = "CohortSubsetQb",
  inherit = QueryBuilder,
  public = list(
    #' return unqiue identifier to be used in the resulting subset query based on operator id
    #' This is required because the subset operation may specify multiple cohorts to subset to
    getTableObjectId = function() {
      return(paste0("S_", private$operatorId))
    },

    getJoinStatements = function() {
      SqlRender::render(
        " JOIN @cohort_database_schema.@cohort_table @o_id ON T.subject_id = @o_id.subject_id",
        o_id = self$getTableObjectId())
    },

    getLogic = function() {
      SqlRender::render(
        "
       AND @o_id.cohort_definition_id IN (@subset_cohort_ids)
       AND (
        @o_id.cohort_start_date >= DATEADD(d, @start_window_start_day, T.@start_window_anchor)
        AND @o_id.cohort_start_date <= DATEADD(d, @start_window_end_day, T.@start_window_anchor)
       )
       AND (
        @o_id.cohort_end_date >= DATEADD(d, @end_window_start_day, T.@end_window_anchor)
        AND @o_id.cohort_end_date <= DATEADD(d, @end_window_end_day, T.@end_window_anchor)
       )",
        o_id = self$getTableObjectId(),
        subset_cohort_ids = private$operator$cohortIds,
        end_window_anchor = ifelse(private$operator$endWindow$targetAnchor == "cohortStart", yes = "cohort_start_date", no = "cohort_end_date"),
        end_window_end_day = private$operator$endWindow$endDay,
        end_window_start_day = private$operator$endWindow$startDay,
        start_window_anchor = ifelse(private$operator$startWindow$targetAnchor == "cohortStart", yes = "cohort_start_date", no = "cohort_end_date"),
        start_window_end_day = private$operator$startWindow$endDay,
        start_window_start_day = private$operator$startWindow$startDay
      )
    },

    getHavingClauses = function() {
      SqlRender::render("COUNT (DISTINCT @o_id.COHORT_DEFINITION_ID) >= @subset_length",
                        o_id = self$getTableObjectId(),
                        subset_length = ifelse(private$operator$cohortCombinationOperator == "any", yes = 1, no = length(private$operator$cohortIds)))
    }
  )
)

LimitSubsetQb <- R6::R6Class(
  classname = "LimitSubsetQb",
  inherit = QueryBuilder,
  public = list()
)

DemographicSubsetQb <- R6::R6Class(
  classname = "LimitSubsetQb",
  inherit = QueryBuilder,
  public = list()
)