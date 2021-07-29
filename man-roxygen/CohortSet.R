#' @param cohortSet   The \code{cohortSet} argument must be a data frame with 
#'                    the following columns: \describe{\item{cohortId}{The 
#'                    unique integer identifier of the cohort} 
#'                    \item{cohortFullName}{The cohort's full name}
#'                    \item{sql}{The OHDSI-SQL used to instantiate the cohort}
#'                    \item{json}{The json column must represent a Circe cohort 
#'                    definition. This field is only required when you would 
#'                    like to generate a cohort that includes inclusion statistics
#'                    since the names of the inclusion rules are parsed from 
#'                    this JSON property.} }
