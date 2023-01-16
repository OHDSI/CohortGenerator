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
  private =  list(
    operator = NULL
  ),
  public = list(
    initialize = function(operator) {
      checkmate::assertR6(operator, "SubsetOperator")
      private$operator <- operator
    },

    #' Get Join Statments for subsetting query
    getJoinStatments = function () {
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
  public = list()
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