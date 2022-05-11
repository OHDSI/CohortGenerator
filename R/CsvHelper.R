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
#' CSV files across the HADES ecosystem. This function may also raise warnings
#' if the data is stored in a format that will not work with the HADES standard
#' for uploading to a results database.
#' 
#' @param x  A data frame or tibble to write to disk.
#' 
#' @param file  The csv file to write.
#' 
#' @param warnOnUploadRuleViolations When TRUE, this function will provide
#' warning messages that may indicate if the data is stored in a format in the
#' CSV that may cause problems when uploading to a database.
#' 
#' @return 
#'  Returns the input x invisibly.
#'
#' @export
writeCsv <- function(x, file, warnOnUploadRuleViolations = TRUE) {
  if (warnOnUploadRuleViolations) {
    # Check the data.frame to ensure it is in the proper format for upload
    isFormattedForDatabaseUpload(x)
    
    # Check if the file name is in lower snake case format
    fileName <- gsub(x = basename(file), pattern = ".csv", replacement = "")
    if (!isSnakeCase(fileName)) {
      warning(paste("The filename:", basename(file), "is not in snake case format"))
    }
    
    # Also perform a check to see if the fileName end in "s" which might indicate
    # that the resulting file name is plural
    if (endsWith(fileName, "s")) {
      message(paste("The filename:", basename(file), "may be plural since it ends in 's'. Please ensure you are using singular nouns for your file names."))
    }
  }
  
  # Write the file
  readr::write_csv(x = x,
                   file = file)
  invisible(x)
}


#' Is the data.frame formatted for uploading to a database?
#'
#' @description
#' This function is used to check a data.frame to ensure all 
#' column names are in snake case format. 
#' 
#' @param x  A data frame
#' @param warn When TRUE, display a warning of any columns are not in snake 
#' case format
#' 
#' @return 
#' Returns TRUE if all columns are snake case format. If warn == TRUE,
#' the function will emit a warning on the column names that are not in snake 
#' case format.
#'
#' @export
isFormattedForDatabaseUpload <- function(x, warn = TRUE) {
  checkmate::assert_data_frame(x)
  columnNames <- colnames(x)
  columnNamesInSnakeCaseFormat <- isSnakeCase(columnNames)
  if (!all(columnNamesInSnakeCaseFormat) && warn) {
    problemColumns <- columnNames[!columnNamesInSnakeCaseFormat]
    problemColumnsWarning <- paste(problemColumns, collapse = ", ")
    problemColumnsFixedWarning <- paste(snakecase::to_snake_case(problemColumns), collapse = ", ")
    warning(paste("The following data.frame column names are not in snake case format:", problemColumnsWarning, "\n  Consider revising the column names to:", problemColumnsFixedWarning))
  }
  return(all(columnNamesInSnakeCaseFormat))
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
  y <- snakecase::to_snake_case(x, numerals = "asis")
  return(x == y)
}