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


QueryBuilder <- R6::R6Class(
  classname = "QueryBuilder",
  private = list(
    operator = NULL,
    id = NULL,
    innerQuery = function(targetTable) {
      stop("Error: not implemented for base class")
    }
  ),
  public = list(
    initialize = function(operator, id) {
      checkmate::assertR6(operator, "SubsetOperator")
      checkmate::assertInt(id, lower = 0)
      private$operator <- operator
      private$id <- id
    },
    getTableObjectId = function() {
      return(paste0("#S_", private$id))
    },
    getQuery = function(targetTable) {
      sql <- SqlRender::render("DROP TABLE IF EXISTS @object_id;\n @inner_query;",
        object_id = self$getTableObjectId(),
        inner_query = private$innerQuery(targetTable)
      )
      return(sql)
    }
  )
)

CohortSubsetQb <- R6::R6Class(
  classname = "CohortSubsetQb",
  inherit = QueryBuilder,
  private = list(
    innerQuery = function(targetTable) {
      sql <- SqlRender::readSql(system.file("sql", "sql_server", "subsets", "CohortSubsetOperator.sql", package = "CohortGenerator"))
      sql <- SqlRender::render(sql,
        target_table = targetTable,
        output_table = self$getTableObjectId(),
        end_window_anchor = ifelse(private$operator$endWindow$targetAnchor == "cohortStart",
          yes = "cohort_start_date",
          no = "cohort_end_date"
        ),
        end_window_end_day = private$operator$endWindow$endDay,
        end_window_start_day = private$operator$endWindow$startDay,
        negate = ifelse(private$operator$negate == TRUE, yes = "1", no = "0"),
        start_window_anchor = ifelse(private$operator$startWindow$targetAnchor == "cohortStart",
          yes = "cohort_start_date",
          no = "cohort_end_date"
        ),
        start_window_end_day = private$operator$startWindow$endDay,
        start_window_start_day = private$operator$startWindow$startDay,
        cohort_ids = private$operator$cohortIds,
        subset_length = ifelse(private$operator$cohortCombinationOperator == "any",
          yes = 1,
          no = length(private$operator$cohortIds)
        ),
        warnOnMissingParameters = TRUE
      )
      return(sql)
    }
  )
)

LimitSubsetQb <- R6::R6Class(
  classname = "LimitSubsetQb",
  inherit = QueryBuilder,
  private = list(
    innerQuery = function(targetTable) {
      sql <- SqlRender::readSql(system.file("sql", "sql_server", "subsets", "LimitSubsetOperator.sql", package = "CohortGenerator"))
      sql <- SqlRender::render(sql,
        calendar_end_date = ifelse(is.null(private$operator$calendarEndDate), yes = "0", no = "1"),
        calendar_end_date_day = ifelse(is.null(private$operator$calendarEndDate), yes = "", no = lubridate::day(private$operator$calendarEndDate)),
        calendar_end_date_month = ifelse(is.null(private$operator$calendarEndDate), yes = "", no = lubridate::month(private$operator$calendarEndDate)),
        calendar_end_date_year = ifelse(is.null(private$operator$calendarEndDate), yes = "", no = lubridate::year(private$operator$calendarEndDate)),
        calendar_start_date = ifelse(is.null(private$operator$calendarStartDate), yes = "0", no = "1"),
        calendar_start_date_day = ifelse(is.null(private$operator$calendarStartDate), yes = "", no = lubridate::day(private$operator$calendarStartDate)),
        calendar_start_date_month = ifelse(is.null(private$operator$calendarStartDate), yes = "", no = lubridate::month(private$operator$calendarStartDate)),
        calendar_start_date_year = ifelse(is.null(private$operator$calendarStartDate), yes = "", no = lubridate::year(private$operator$calendarStartDate)),
        follow_up_time = private$operator$followUpTime,
        limit_to = private$operator$limitTo,
        prior_time = private$operator$priorTime,
        use_prior_fu_time = private$operator$followUpTime > 0 || private$operator$priorTime > 0,
        output_table = self$getTableObjectId(),
        target_table = targetTable,
        warnOnMissingParameters = TRUE
      )
      return(sql)
    }
  )
)

DemographicSubsetQb <- R6::R6Class(
  classname = "DemographicSubsetQb",
  inherit = QueryBuilder,
  private = list(
    innerQuery = function(targetTable) {
      sql <- SqlRender::readSql(system.file("sql", "sql_server", "subsets", "DemographicSubsetOperator.sql", package = "CohortGenerator"))
      sql <- SqlRender::render(sql,
        target_table = targetTable,
        output_table = self$getTableObjectId(),
        age_min = private$operator$ageMin,
        age_max = private$operator$ageMax,
        gender_concept_id = private$operator$getGender(),
        race_concept_id = private$operator$getRace(),
        ethnicity_concept_id = private$operator$getEthnicity(),
        warnOnMissingParameters = TRUE
      )
      return(sql)
    }
  )
)
