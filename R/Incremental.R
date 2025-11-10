# Copyright 2025 Observational Health Data Sciences and Informatics
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
#'
#' @param val   The value to hash. It is converted to a character to perform
#'              the hash.
#'
#' @return
#' Returns a string containing the checksum
#' @family utils
#' @export
computeChecksum <- function(val) {
  val <- as.character(val)
  # strip whitespace
  val <- gsub("[\r\n]", "", val)
  val <- trimws(val)
  hashes <- sapply(val, digest::digest, algo = "md5", serialize = FALSE, USE.NAMES = FALSE)
  return(hashes)
}
