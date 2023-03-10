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

#' Create an empty cohort definition set
#'
#' @description
#' This function creates an empty cohort set data.frame for use
#' with \code{generateCohortSet}.
#'
#' @param verbose When TRUE, descriptions of each field in the data.frame are
#'                returned
#'
#' @return
#' Invisibly returns an empty cohort set data.frame
#'
#' @export
createEmptyCohortDefinitionSet <- function(verbose = FALSE) {
  checkmate::assert_logical(verbose)
  cohortDefinitionSetSpec <- .getCohortDefinitionSetSpecification()
  if (verbose) {
    print(cohortDefinitionSetSpec)
  }
  # Build the data.frame dynamically from the cohort definition set spec
  df <- .createEmptyDataFrameFromSpecification(cohortDefinitionSetSpec)
  invisible(df)
}

#' Is the data.frame a cohort definition set?
#'
#' @description
#' This function checks a data.frame to verify it holds the expected format
#' for a cohortDefinitionSet.
#'
#' @param x  The data.frame to check
#'
#' @return
#' Returns TRUE if the input is a cohortDefinitionSet or returns FALSE
#' with warnings on any violations
#'
#' @export
isCohortDefinitionSet <- function(x) {
  columnNamesMatch <- .cohortDefinitionSetHasRequiredColumns(x = x, emitWarning = TRUE)
  dataTypesMatch <- FALSE
  if (columnNamesMatch) {
    dataTypesMatch <- checkAndFixCohortDefinitionSetDataTypes(
      x = x,
      fixDataTypes = FALSE,
      emitWarning = TRUE
    )$dataTypesMatch
  }
  return(columnNamesMatch && dataTypesMatch)
}

#' Check if a cohort definition set is using the proper data types
#'
#' @description
#' This function checks a data.frame to verify it holds the expected format
#' for a cohortDefinitionSet's data types and can optionally fix data types
#' that do not match the specification.
#'
#' @param x  The cohortDefinitionSet data.frame to check
#'
#' @param fixDataTypes When TRUE, this function will attempt to fix the data types
#'                     to match the specification. @seealso [createEmptyCohortDefinitionSet()].
#'
#' @param emitWarning  When TRUE, this function will emit warning messages when problems are
#'                     encountered.
#'
#' @return
#' Returns a list() of the following form:
#'
#' list(
#'    dataTypesMatch = TRUE/FALSE,
#'    x = data.frame()
#' )
#'
#' dataTypesMatch == TRUE when the supplied data.frame x matches the cohortDefinitionSet
#' specification's data types.
#'
#' If fixDataTypes == TRUE, x will hold the original data from x with the
#' data types corrected. Otherwise x will hold the original value passed to this
#' function.
#'
#' @export
checkAndFixCohortDefinitionSetDataTypes <- function(x, fixDataTypes = TRUE, emitWarning = FALSE) {
  checkmate::assert_data_frame(x)
  df <- createEmptyCohortDefinitionSet(verbose = FALSE)
  cohortDefinitionSetColumns <- colnames(df)
  cohortDefinitionSetSpec <- .getCohortDefinitionSetSpecification()

  columnNamesMatch <- .cohortDefinitionSetHasRequiredColumns(x = x, emitWarning = emitWarning)
  if (!columnNamesMatch) {
    stop("Cannot check and fix cohortDefinitionSet since it is missing required columns.")
  }

  # Compare the data types from the input x to an empty cohort
  # definition set to ensure the same data types are present
  dataTypesMatch <- FALSE
  # Subset x to the required columns
  xSubset <- x[, cohortDefinitionSetColumns]
  # Get the data types
  xDataTypes <- sapply(xSubset, typeof)
  # Get the reference data types
  cohortDefinitionSetDataTypes <- sapply(df, typeof)
  # Check if the data types match
  dataTypesMatch <- identical(x = xDataTypes, y = cohortDefinitionSetDataTypes)
  if (!dataTypesMatch && emitWarning) {
    dataTypesMismatch <- setdiff(x = cohortDefinitionSetDataTypes, y = xDataTypes)
    # Create a column for the warning message
    cohortDefinitionSetSpec$columnNameWithDataType <- paste(cohortDefinitionSetSpec$columnName, cohortDefinitionSetSpec$dataType, sep = " == ")
    userSuppliedCohortDefinitionSetDataTypes <- paste(names(x[1 == 0, ]), "==", sapply(x[1 == 0, ], class), collapse = "\n")
    warningMessage <- paste0("Your cohortDefinitionSet had a mismatch in data types. Please check your cohortDefinitionSet to ensure it conforms to the following expected data types:")
    warningMessage <- paste0(warningMessage, "Expected column == data type\n--------------------------\n", paste(cohortDefinitionSetSpec$columnNameWithDataType, collapse = "\n"))
    warningMessage <- paste0(warningMessage, "\n--------------------------\n")
    warningMessage <- paste0(warningMessage, "Your cohortDefinitionSet \n--------------------------\n", userSuppliedCohortDefinitionSetDataTypes)
    warning(warningMessage)
  }

  # If fixDataTypes, change the data types of the data.frame to
  # match the specification
  if (!dataTypesMatch && fixDataTypes) {
    for (i in 1:nrow(cohortDefinitionSetSpec)) {
      colName <- cohortDefinitionSetSpec$columnName[i]
      dataType <- paste0("as.", cohortDefinitionSetSpec$dataType[i])
      x[[colName]] <- do.call(what = dataType, args = as.list(x[[colName]]))
    }
  }

  return(list(
    dataTypesMatch = dataTypesMatch,
    x = x
  ))
}

.cohortDefinitionSetHasRequiredColumns <- function(x, emitWarning = FALSE) {
  checkmate::assert_data_frame(x)
  df <- createEmptyCohortDefinitionSet(verbose = FALSE)
  cohortDefinitionSetSpec <- .getCohortDefinitionSetSpecification()

  # Compare the column names from the input x to an empty cohort
  # definition set to ensure the required columns are present
  cohortDefinitionSetColumns <- colnames(df)
  matchingColumns <- intersect(x = colnames(x), y = cohortDefinitionSetColumns)
  columnNamesMatch <- setequal(matchingColumns, cohortDefinitionSetColumns)

  if (!columnNamesMatch && emitWarning) {
    columnsMissing <- setdiff(x = cohortDefinitionSetColumns, y = colnames(x))
    warningMessage <- paste0(
      "The following columns were missing in your cohortDefinitionSet: ",
      paste(columnsMissing, collapse = ","),
      ". A cohortDefinitionSet requires the following columns: ",
      paste(cohortDefinitionSetColumns, collapse = ",")
    )
    warning(warningMessage)
  }
  invisible(columnNamesMatch)
}

#' Helper function to return the specification description of a
#' cohortDefinitionSet
#'
#' @description
#' This function reads from the cohortDefinitionSetSpecificationDescription.csv
#' to return a data.frame that describes the required columns in a
#' cohortDefinitionSet
#'
#' @return
#' Returns a data.frame that defines a cohortDefinitionSet
#'
#' @noRd
#' @keywords internal
.getCohortDefinitionSetSpecification <- function() {
  return(readCsv(system.file("cohortDefinitionSetSpecificationDescription.csv",
    package = "CohortGenerator",
    mustWork = TRUE
  )))
}

#' Get a cohort definition set
#'
#' @description
#' This function supports the legacy way of retrieving a cohort definition set
#' from the file system or in a package. This function supports the legacy way of
#' storing a cohort definition set in a package with a CSV file, JSON files,
#' and SQL files in the `inst` folder.
#'
#' @param settingsFileName The name of the CSV file that will hold the cohort information
#'                         including the cohortId and cohortName
#'
#' @param jsonFolder       The name of the folder that will hold the JSON representation
#'                         of the cohort if it is available in the cohortDefinitionSet
#'
#' @param sqlFolder        The name of the folder that will hold the SQL representation
#'                         of the cohort.
#'
#' @param cohortFileNameFormat  Defines the format string  for naming the cohort
#'                              JSON and SQL files. The format string follows the
#'                              standard defined in the base sprintf function.
#'
#' @param cohortFileNameValue   Defines the columns in the cohortDefinitionSet to use
#'                              in conjunction with the cohortFileNameFormat parameter.
#'
#' @param subsetJsonFolder      Defines the folder to store the subset JSON
#'
#' @param packageName The name of the package containing the cohort definitions.
#'
#' @param warnOnMissingJson Provide a warning if a .JSON file is not found for a
#'                          cohort in the settings file
#'
#' @param verbose           When TRUE, extra logging messages are emitted
#'
#' @return
#' Returns a cohort set data.frame
#'
#' @export
getCohortDefinitionSet <- function(settingsFileName = "Cohorts.csv",
                                   jsonFolder = "cohorts",
                                   sqlFolder = "sql/sql_server",
                                   cohortFileNameFormat = "%s",
                                   cohortFileNameValue = c("cohortId"),
                                   subsetJsonFolder = "inst/cohort_subset_definitions/",
                                   packageName = NULL,
                                   warnOnMissingJson = TRUE,
                                   verbose = FALSE) {
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)

  getPath <- function(fileName) {
    path <- fileName
    if (!is.null(packageName)) {
      path <- system.file(fileName, package = packageName)
    }
    if (verbose) {
      ParallelLogger::logInfo(paste0(" -- Loading ", basename(fileName), " from ", path))
    }
    if (!file.exists(path)) {
      if (grepl(".json$", tolower(basename(fileName))) && warnOnMissingJson) {
        errorMsg <- ifelse(is.null(packageName),
          paste0("File not found: ", path),
          paste0("File, ", fileName, " not found in package: ", packageName)
        )
        warning(errorMsg)
      }
    }
    return(path)
  }

  # Read the settings file which holds the cohortDefinitionSet
  ParallelLogger::logInfo("Loading cohortDefinitionSet")
  settings <- readCsv(file = getPath(fileName = settingsFileName), warnOnCaseMismatch = FALSE)

  assert_settings_columns(names(settings), getPath(fileName = settingsFileName))
  checkmate::assert_true(all(cohortFileNameValue %in% names(settings)))
  checkmate::assert_true((!all(.getFileDataColumns() %in% names(settings))))

  readFile <- function(fileName) {
    if (file.exists(fileName)) {
      return(SqlRender::readSql(fileName))
    } else {
      if (grepl(".json$", tolower(basename(fileName))) && warnOnMissingJson) {
        warning(paste0(" --- ", fileName, " not found"))
        return(NA)
      } else {
        stop(paste0("File not found: ", fileName))
      }
    }
  }

  loadSubsets <- FALSE
  subsetsToLoad <- data.frame()
  # Do not attempt to load subset definition
  if ("isSubset" %in% colnames(settings)) {
    subsetsToLoad <- settings %>%
      dplyr::filter(.data$isSubset)

    settings <- settings %>%
      dplyr::filter(!.data$isSubset)

    loadSubsets <- TRUE
  }

  # Read the JSON/SQL files
  fileData <- data.frame()
  for (i in 1:nrow(settings)) {
    cohortFileNameRoot <- .getFileNameFromCohortDefinitionSet(
      cohortDefinitionSetRow = settings[i, ],
      cohortFileNameValue = cohortFileNameValue,
      cohortFileNameFormat = cohortFileNameFormat
    )
    cohortFileNameRoot <- .removeNonAsciiCharacters(cohortFileNameRoot)
    json <- readFile(fileName = getPath(fileName = file.path(jsonFolder, paste0(cohortFileNameRoot, ".json"))))
    sql <- readFile(fileName = getPath(fileName = file.path(sqlFolder, paste0(cohortFileNameRoot, ".sql"))))
    fileData <- rbind(fileData, data.frame(
      json = json,
      sql = sql
    ))
  }

  cohortDefinitionSet <- cbind(settings, fileData)
  # Loading cohort subset definitions with their associated targets
  if (loadSubsets & nrow(subsetsToLoad) > 0) {
    if (dir.exists(subsetJsonFolder)) {
      ParallelLogger::logInfo("Loading Cohort Subset Definitions")

      ## Loading subsets that apply to the saved definition sets
      for (i in unique(subsetsToLoad$subsetDefinitionId)) {
        subsetFile <- file.path(subsetJsonFolder, paste0(i, ".json"))
        ParallelLogger::logInfo("Loading Cohort Subset Defintion ", subsetFile)
        subsetDef <- CohortSubsetDefinition$new(ParallelLogger::loadSettingsFromJson(subsetFile))
        # Find target cohorts for this subset definition
        subsetTargetIds <- unique(subsetsToLoad[subsetsToLoad$subsetDefinitionId == i, ]$subsetParent)

        cohortDefinitionSet <- addCohortSubsetDefinition(cohortDefinitionSet,
          subsetDef,
          targetCohortIds = subsetTargetIds
        )
      }
    } else {
      stop("subset definitions defined in settings file but no corresponding subset definition file is associated")
    }
  }

  invisible(cohortDefinitionSet)
}

#' Save the cohort definition set to the file system
#'
#' @description
#' This function saves a cohortDefinitionSet to the file system and provides
#' options for specifying where to write the individual elements: the settings
#' file will contain the cohort information as a CSV specified by the
#' settingsFileName, the cohort JSON is written to the jsonFolder and the SQL
#' is written to the sqlFolder. We also provide a way to specify the
#' json/sql file name format using the cohortFileNameFormat and
#' cohortFileNameValue parameters.
#'
#' @template CohortDefinitionSet
#'
#' @param settingsFileName The name of the CSV file that will hold the cohort information
#'                         including the cohortId and cohortName
#'
#' @param jsonFolder       The name of the folder that will hold the JSON representation
#'                         of the cohort if it is available in the cohortDefinitionSet
#'
#' @param sqlFolder        The name of the folder that will hold the SQL representation
#'                         of the cohort.
#'
#' @param cohortFileNameFormat  Defines the format string  for naming the cohort
#'                              JSON and SQL files. The format string follows the
#'                              standard defined in the base sprintf function.
#'
#' @param cohortFileNameValue   Defines the columns in the cohortDefinitionSet to use
#'                              in conjunction with the cohortFileNameFormat parameter.
#'
#' @param subsetJsonFolder      Defines the folder to store the subset JSON
#'
#' @param verbose           When TRUE, logging messages are emitted to indicate export
#'                          progress.
#'
#' @export
saveCohortDefinitionSet <- function(cohortDefinitionSet,
                                    settingsFileName = "inst/Cohorts.csv",
                                    jsonFolder = "inst/cohorts",
                                    sqlFolder = "inst/sql/sql_server",
                                    cohortFileNameFormat = "%s",
                                    cohortFileNameValue = c("cohortId"),
                                    subsetJsonFolder = "inst/cohort_subset_definitions/",
                                    verbose = FALSE) {
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assert_vector(cohortFileNameValue)
  checkmate::assert_true(length(cohortFileNameValue) > 0)
  assert_settings_columns(names(cohortDefinitionSet))
  checkmate::assert_true(all(cohortFileNameValue %in% names(cohortDefinitionSet)))
  settingsFolder <- dirname(settingsFileName)
  if (!dir.exists(settingsFolder)) {
    dir.create(settingsFolder, recursive = TRUE)
  }
  if (!file.exists(jsonFolder)) {
    dir.create(jsonFolder, recursive = TRUE)
  }
  if (!file.exists(sqlFolder)) {
    dir.create(sqlFolder, recursive = TRUE)
  }

  # Export the cohortDefinitionSet to the settings folder
  if (verbose) {
    ParallelLogger::logInfo("Exporting cohortDefinitionSet to ", settingsFileName)
  }
  # Write the settings file and ensure that the "sql" and "json" columns are
  # not included
  writeCsv(
    x = cohortDefinitionSet[, -which(names(cohortDefinitionSet) %in% .getFileDataColumns())],
    file = settingsFileName,
    warnOnUploadRuleViolations = FALSE
  )

  hasSubsets <- hasSubsetDefinitions(cohortDefinitionSet)
  # Export the SQL & JSON for each entry
  for (i in 1:nrow(cohortDefinitionSet)) {
    cohortId <- cohortDefinitionSet$cohortId[i]
    cohortName <- .removeNonAsciiCharacters(cohortDefinitionSet$cohortName[i])
    json <- ifelse("json" %in% names(cohortDefinitionSet), .removeNonAsciiCharacters(cohortDefinitionSet$json[i]), "{}")
    sql <- cohortDefinitionSet$sql[i]
    fileNameRoot <- .getFileNameFromCohortDefinitionSet(
      cohortDefinitionSetRow = cohortDefinitionSet[i, ],
      cohortFileNameValue = cohortFileNameValue,
      cohortFileNameFormat = cohortFileNameFormat
    )

    if (hasSubsets && cohortDefinitionSet$isSubset[i]) {
      next # Subsets are saved only as json
    }

    if (verbose) {
      ParallelLogger::logInfo("Exporting (", i, "/", nrow(cohortDefinitionSet), "): ", cohortName)
    }

    if (!is.na(json) && nchar(json) > 0) {
      SqlRender::writeSql(sql = json, targetFile = file.path(jsonFolder, paste0(fileNameRoot, ".json")))
    }

    SqlRender::writeSql(sql = sql, targetFile = file.path(sqlFolder, paste0(fileNameRoot, ".sql")))
  }

  if (hasSubsets) {
    for (subsetDefinition in attr(cohortDefinitionSet, "cohortSubsetDefinitions")) {
      saveCohortSubsetDefinition(subsetDefinition, subsetJsonFolder)
    }
  }

  ParallelLogger::logInfo("Cohort definition saved")
}

.getSettingsFileRequiredColumns <- function() {
  return(c("cohortId", "cohortName"))
}

.getFileDataColumns <- function() {
  return(c("json", "sql"))
}

.getFileNameFromCohortDefinitionSet <- function(cohortDefinitionSetRow,
                                                cohortFileNameValue,
                                                cohortFileNameFormat) {
  checkmate::assertDataFrame(cohortDefinitionSetRow, min.rows = 1, max.rows = 1, col.names = "named")
  # Create the list of arguments to pass to stri_sprintf
  # to create the file name
  argList <- list(format = cohortFileNameFormat)
  for (j in 1:length(cohortFileNameValue)) {
    argList <- append(argList, cohortDefinitionSetRow[1, cohortFileNameValue[j]][[1]])
  }
  fileNameRoot <- do.call(stringi::stri_sprintf, argList)
  return(fileNameRoot)
}

.removeNonAsciiCharacters <- function(expression) {
  return(stringi::stri_trans_general(expression, "latin-ascii"))
}

#' Custom checkmate assertion for ensuring the settings columns are properly
#' specified
#'
#' @description
#' This function is used to provide a more informative message when ensuring
#' that the columns in the cohort definition set or the CSV file that
#' defines the cohort definition set is properly specified. This function
#' is then bootstrapped upon package initialization (code in CohortGenerator.R)
#' to allow for it to work with the other checkmate assertions as described in:
#' https://mllg.github.io/checkmate/articles/checkmate.html. The assertion function
#' is called assert_settings_columns.
#'
#' @param columnNames The name of the columns found in either the cohortDefinitionSet
#'                    data frame or from reading the contents of the settingsFile
#'
#' @param settingsFileName The file name of the CSV that defines the cohortDefinitionSet.
#'                         When NULL, this function assumes the column names are defined
#'                         in a data.frame representation of the cohortDefinitionSet
#' @return
#' Returns TRUE if all required columns are found otherwise it returns an error
#' @noRd
#' @keywords internal
checkSettingsColumns <- function(columnNames, settingsFileName = NULL) {
  settingsColumns <- .getSettingsFileRequiredColumns()
  res <- all(settingsColumns %in% columnNames)
  if (!isTRUE(res)) {
    sourceDescription <- "cohort definition set"
    if (!is.null(settingsFileName)) {
      sourceDescription <- "settings file"
    }
    errorMessage <- paste0("CohortGenerator requires the following columns in the ", sourceDescription, ": ", paste(shQuote(settingsColumns), collapse = ", "), ". The following columns were found: ", paste(shQuote(columnNames), collapse = ", "))
    return(errorMessage)
  } else {
    return(TRUE)
  }
}

.createEmptyDataFrameFromSpecification <- function(specifications) {
  # Build the data.frame dynamically from the cohort definition set spec
  df <- data.frame()
  for (i in 1:nrow(specifications)) {
    colName <- specifications$columnName[i]
    dataType <- specifications$dataType[i]
    if (dataType == "integer64") {
      df <- df %>% dplyr::mutate(!!colName := do.call(what = bit64::as.integer64, args = list()))
    } else {
      df <- df %>% dplyr::mutate(!!colName := do.call(what = dataType, args = list()))
    }
  }
  invisible(df)
}
