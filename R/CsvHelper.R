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


#' Used to read a .csv file
#'
#' @description
#' This function is used to centralize the function for reading
#' .csv files across the HADES ecosystem. This function will automatically
#' convert from snake_case in the file to camelCase in the data.frame returned
#' as is the standard described in:
#' https://ohdsi.github.io/Hades/codeStyle.html#Interfacing_between_R_and_SQL
#'
#' @param file  The .csv file to read.
#' @param warnOnCaseMismatch  When TRUE, raise a warning if column headings
#' in the .csv are not in snake_case format
#'
#' @return
#' A tibble with the .csv contents
#'
#' @export
readCsv <- function(file, warnOnCaseMismatch = TRUE) {
  fileContents <- .readCsv(file = file)
  columnNames <- colnames(fileContents)
  columnNamesInSnakeCaseFormat <- isSnakeCase(columnNames)
  if (!all(columnNamesInSnakeCaseFormat) && warnOnCaseMismatch) {
    problemColumns <- columnNames[!columnNamesInSnakeCaseFormat]
    problemColumnsWarning <- paste(problemColumns, collapse = ", ")
    warning(paste("The following columns were not in snake case format:", problemColumnsWarning))
  }
  colIdxToConvert <- which(isSnakeCase(columnNames))
  names(fileContents)[colIdxToConvert] <- SqlRender::snakeCaseToCamelCase(names(fileContents)[colIdxToConvert])
  invisible(fileContents)
}

#' Internal read CSV file when control over column formatting is required.
#'
#' @description
#' This function is internal to the package
#'
#' @param file  The .csv file to read.
#'
#' @return
#'  Returns the file contents invisibly.
#'
#' @noRd
#' @keywords internal
.readCsv <- function(file) {
  invisible(readr::read_csv(
    file = file,
    col_types = readr::cols(),
    lazy = FALSE
  ))
}

#' Used to write a .csv file
#'
#' @description
#' This function is used to centralize the function for writing
#' .csv files across the HADES ecosystem. This function will automatically
#' convert from camelCase in the data.frame to snake_case column names
#' in the resulting .csv file as is the standard
#' described in:
#' https://ohdsi.github.io/Hades/codeStyle.html#Interfacing_between_R_and_SQL
#'
#' This function may also raise warnings if the data is stored in a format
#' that will not work with the HADES standard for uploading to a results database.
#' Specifically file names should be in snake_case format, all column headings
#' are in snake_case format and where possible the file name should not be plural.
#' See \code{isFormattedForDatabaseUpload} for a helper function to check a
#' data.frame for rules on the column names
#'
#' @param x  A data frame or tibble to write to disk.
#'
#' @param file  The .csv file to write.
#'
#' @param append When TRUE, append the values of x to an existing file.
#'
#' @param warnOnCaseMismatch  When TRUE, raise a warning if columns in the
#' data.frame are NOT in camelCase format.
#'
#' @param warnOnFileNameCaseMismatch When TRUE, raise a warning if the file
#' name specified is not in snake_case format.
#'
#' @param warnOnUploadRuleViolations When TRUE, this function will provide
#' warning messages that may indicate if the data is stored in a format in the
#' .csv that may cause problems when uploading to a database.
#'
#' @return
#'  Returns the input x invisibly.
#'
#' @export
writeCsv <- function(x, file, append = FALSE, warnOnCaseMismatch = TRUE, warnOnFileNameCaseMismatch = TRUE, warnOnUploadRuleViolations = TRUE) {
  columnNames <- colnames(x)
  columnNamesInCamelCaseFormat <- isCamelCase(columnNames)
  if (!all(columnNamesInCamelCaseFormat) && warnOnCaseMismatch) {
    problemColumns <- columnNames[!columnNamesInCamelCaseFormat]
    problemColumnsWarning <- paste(problemColumns, collapse = ", ")
    warning(paste("The following columns were not in camel case format:", problemColumnsWarning))
  }
  colnames(x) <- SqlRender::camelCaseToSnakeCase(colnames(x))

  if (warnOnUploadRuleViolations) {
    # Check the data.frame to ensure it is in the proper format for upload
    isFormattedForDatabaseUpload(x)

    # Check if the file name is in lower snake case format
    fileName <- gsub(x = basename(file), pattern = ".csv", replacement = "")
    if (!isSnakeCase(fileName) && warnOnFileNameCaseMismatch) {
      warning(paste("The filename:", basename(file), "is not in snake case format"))
    }
  }

  # Write the file
  .writeCsv(x = x, file = file, append = append)
}


#' Internal write.csv file when control over column formatting is required.
#'
#' @description
#' This function is internal to the package since the \code{exportCohortStatsTables}
#' requires additional control over the column formatting which requires that
#' we bypass the \code{writeCsv} function since it will automatically convert
#' from camelCase to snake_case.
#'
#' @param x  A data frame or tibble to write to disk.
#'
#' @param file  The .csv file to write.
#'
#' @param append When TRUE, append the values of x to an existing file.
#'
#' @return
#'  Returns the input x invisibly.
#'
#' @noRd
#' @keywords internal
.writeCsv <- function(x, file, append = FALSE) {
  # Write the file
  readr::write_csv(
    x = x,
    file = file,
    append = append
  )
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
    # Are the problem columns in camelCase? If so, we can use SqlRender
    # to provide a suggestion
    columnNameSuggestions <- c()
    if (length(problemColumns) > 0) {
      for (i in 1:length(problemColumns)) {
        suggestion <- problemColumns[i]
        if (isCamelCase(suggestion)) {
          suggestion <- SqlRender::camelCaseToSnakeCase(suggestion)
        }
        columnNameSuggestions <- c(columnNameSuggestions, suggestion)
      }
    }
    problemColumnsWarning <- paste(problemColumns, collapse = ", ")
    problemColumnsSuggestions <- paste(columnNameSuggestions, collapse = ", ")
    warning(paste("The following data.frame column names are not in snake case format:", problemColumnsWarning, "\n  Consider revising the column names to:", problemColumnsSuggestions))
  }
  return(all(columnNamesInSnakeCaseFormat))
}

#' Used to check if a string is in snake case
#'
#' @description
#' This function is used check if a string conforms to
#' the snake case format.
#'
#' @param x  The string to evaluate
#'
#' @return
#'  TRUE if the string is in snake case
#'
#' @export
isSnakeCase <- function(x) {
  return(grepl(pattern = "^[a-z0-9]+(?:_[a-z0-9]+)*$", x = x))
}

#' Used to check if a string is in lower camel case
#'
#' @description
#' This function is used check if a string conforms to
#' the lower camel case format.
#'
#' @param x  The string to evaluate
#'
#' @return
#'  TRUE if the string is in lower camel case
#'
#' @export
isCamelCase <- function(x) {
  return(grepl(pattern = "^[a-z]+([A-Z0-9]*[a-z0-9]+[A-Za-z0-9]*)$", x = x))
}
