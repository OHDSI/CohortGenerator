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


#' Used to read a csv file
#'
#' @description
#' This function is used to centralize the function for reading
#' CSV files across the HADES ecosystem.
#' 
#' @param file  The csv file to read.
#'
#' @return
#' A tibble with the CSV contents
#' 
#' @export
readCsv <- function(file) {
  readr::read_csv(file = file, 
                  col_types = readr::cols(), 
                  lazy = FALSE)
}

#' Used to write a csv file
#'
#' @description
#' This function is used to centralize the function for writing
#' CSV files across the HADES ecosystem.
#' 
#' @param x  A data frame or tibble to write to disk.
#' 
#' @param file  The csv file to write.
#'
#' @return 
#'  Returns the input x invisibly.
#'
#' @export
writeCsv <- function(x, file) {
  readr::write_csv(x = x,
                   file = file)
  invisible(x)
}

#' Used to check if a string is in lower camel case
#'
#' @description
#' This function is used check file and field names
#' to ensure the proper casing is in use.
#' 
#' @param x  The string to evaluate
#' 
#' @return 
#'  TRUE if the string is in lower camel case
#'
#' @export
isCamelCase <- function(x) {
  y <- snakecase::to_lower_camel_case(x)
  return(x == y)
}

#' Used to check if a string is in lower snake case
#'
#' @description
#' This function is used check file and field names
#' to ensure the proper casing is in use.
#' 
#' @param x  The string to evaluate
#' 
#' @return 
#'  TRUE if the string is in lower snake case
#'
#' @export
isSnakeCase <- function(x) {
  y <- snakecase::to_snake_case(x)
  return(x == y)
}