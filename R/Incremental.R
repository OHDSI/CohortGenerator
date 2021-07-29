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

#' Computes the checksum for a value
#'
#' @description
#' This is used as part of the incremental operations to hash a value
#' to store in a record keeping file. This function leverages the md5
#' hash from the digest package
#' 
#' @param val   The value to hash. It is converted to a character to perform
#'              the hash.
#'
#' @return
#' Returns a string containing the checksum
#' 
#' @export
computeChecksum <- function(val) {
  return(sapply(as.character(val), digest::digest, algo = "md5", serialize = FALSE))
}

#' Is a task required when running in incremental mode
#'
#' @description
#' This function will attempt to check the \code{recordKeepingFile}
#' to determine if an individual operation has completed by comparing the 
#' keys passed into the function with the checksum supplied
#' 
#' @param ...       Parameter values used to identify the key 
#'                  in the incremental record keeping file
#'                                  
#' @param checksum  The checksum representing the operation to check
#' 
#' @param recordKeepingFile    A file path to a CSV file containing the record
#'                             keeping information.
#'                             
#' @param verbose   When TRUE, this function will output if a particular operation
#'                  has completed based on inspecting the recordKeepingFile.
#'
#' @return
#' Returns TRUE if the operation has completed according to the contents of
#' the record keeping file.
#' 
#' @export
isTaskRequired <- function(..., checksum, recordKeepingFile, verbose = TRUE) {
  if (file.exists(recordKeepingFile)) {
    recordKeeping <- readr::read_csv(recordKeepingFile, col_types = readr::cols())
    task <- recordKeeping[getKeyIndex(list(...), recordKeeping), ]
    if (nrow(task) == 0) {
      return(TRUE)
    }
    if (nrow(task) > 1) {
      stop("Duplicate key ", as.character(list(...)), " found in recordkeeping table")
    }
    if (task$checksum == checksum) {
      if (verbose) {
        key <- list(...)
        key <- paste(sprintf("%s = '%s'", names(key), key), collapse = ", ")
        ParallelLogger::logInfo("Skipping ", key, " because unchanged from earlier run")
      }
      return(FALSE)
    } else {
      return(TRUE)
    }
  } else {
    return(TRUE)
  }
}

#' Get a list of tasks required when running in incremental mode
#'
#' @description
#' This function will attempt to check the \code{recordKeepingFile}
#' to determine if a list of operations have completed by comparing the 
#' keys passed into the function with the checksum supplied
#' 
#' @param ...       Parameter values used to identify the key 
#'                  in the incremental record keeping file
#'                                  
#' @param checksum  The checksum representing the operation to check
#' 
#' @param recordKeepingFile    A file path to a CSV file containing the record
#'                             keeping information.
#'                             
#' @return
#' Returns a list of outstanding tasks based on inspecting the full contents
#' of the record keeping file
#' 
#' @export
getRequiredTasks <- function(..., checksum, recordKeepingFile) {
  tasks <- list(...)
  if (file.exists(recordKeepingFile) && length(tasks[[1]]) > 0) {
    recordKeeping <- readr::read_csv(recordKeepingFile, col_types = readr::cols())
    tasks$checksum <- checksum
    tasks <- tibble::as_tibble(tasks)
    if (all(names(tasks) %in% names(recordKeeping))) {
      idx <- getKeyIndex(recordKeeping[, names(tasks)], tasks)
    } else {
      idx <- c()
    }
    tasks$checksum <- NULL
    if (length(idx) > 0) {
      text <- paste(sprintf("%s = %s", names(tasks), tasks[idx, ]), collapse = ", ")
      ParallelLogger::logInfo("Skipping ", text, " because unchanged from earlier run")
      tasks <- tasks[-idx, ]
    }
  }
  return(tasks)
}

#' Record a task as complete
#'
#' @description
#' This function will record a task as completed in the \code{recordKeepingFile}
#' 
#' @param ...       Parameter values used to identify the key 
#'                  in the incremental record keeping file
#'                                  
#' @param checksum  The checksum representing the operation to check
#' 
#' @param recordKeepingFile    A file path to a CSV file containing the record
#'                             keeping information.
#' @param incremental    When TRUE, this function will record tasks otherwise
#'                       it will return without attempting to perform any action
#'                             
#' @export
recordTasksDone <- function(..., checksum, recordKeepingFile, incremental = TRUE) {
  if (!incremental) {
    return()
  }
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(recordKeepingFile)) {
    recordKeeping <- readr::read_csv(recordKeepingFile, col_types = readr::cols())
    idx <- getKeyIndex(list(...), recordKeeping)
    if (length(idx) > 0) {
      recordKeeping <- recordKeeping[-idx, ]
    }
  } else {
    recordKeeping <- tibble::tibble()
  }
  newRow <- tibble::as_tibble(list(...))
  newRow$checksum <- checksum
  newRow$timeStamp <- Sys.time()
  recordKeeping <- dplyr::bind_rows(recordKeeping, newRow)
  readr::write_csv(recordKeeping, recordKeepingFile)
}

#' Used in incremental mode to save values to a file
#'
#' @description
#' When running in incremental mode, we may need to update results in a CSV
#' file. This function will replace the \code{data} in \code{fileName} based
#' on the key parameters
#' 
#' @param data  The data to record in the file
#' 
#' @param fileName  A CSV holding results in the same structure as the data
#'                  parameter
#'                             
#' @param ...       Parameter values used to identify the key 
#'                  in the results file
#'                             
#' @export
saveIncremental <- function(data, fileName, ...) {
  if (length(list(...)[[1]]) == 0) {
    return()
  }
  if (file.exists(fileName)) {
    previousData <- readr::read_csv(fileName, col_types = readr::cols())
    idx <- getKeyIndex(list(...), previousData)
    if (length(idx) > 0) {
      previousData <- previousData[-idx, ]
    }
    data <- dplyr::bind_rows(previousData, data)
  }
  readr::write_csv(data, fileName)
}

getKeyIndex <- function(key, recordKeeping) {
  if (nrow(recordKeeping) == 0 || length(key[[1]]) == 0 || !all(names(key) %in% names(recordKeeping))) {
    return(c())
  } else {
    key <- unique(tibble::as_tibble(key))
    recordKeeping$idxCol <- 1:nrow(recordKeeping)
    idx <- merge(recordKeeping, key)$idx
    return(idx)
  }
}
