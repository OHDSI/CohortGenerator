#' @param cohortDefinitionSet   The \code{cohortDefinitionSet} argument must be a data frame with 
#'                              the following columns: \describe{
#'                              \item{cohortId}{The unique integer identifier of the cohort} 
#'                              \item{cohortName}{The cohort's name}
#'                              \item{sql}{The OHDSI-SQL used to generate the cohort}}
#'                              Optionally, this data frame may contain: \describe{
#'                              \item{json}{The Circe JSON representation of the cohort}}
