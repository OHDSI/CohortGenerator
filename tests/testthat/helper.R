connectionDetails <- Eunomia::getEunomiaConnectionDetails()

#' Create the Circe cohort expression from a JSON file for generating
#' SQL dynamically
#'
#' @description
#' This function constructs a Circe cohort expression from a JSON file for use
#' with other CirceR functions.
#'
#' @param filePath      The file path containing the Circe JSON file
#'
createCirceExpressionFromFile <- function(filePath) {
  cohortExpression <- readChar(filePath, file.info(filePath)$size)
  return(CirceR::cohortExpressionFromJson(cohortExpression))
}
