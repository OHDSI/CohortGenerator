#' @param connectionDetails       (optional) An object of type \code{connectionDetails} as created
#'                                using the \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                function in the DatabaseConnector package. Can be left NULL if
#'                                \code{connection} is provided.
#' @param connection              (optional) An object of type \code{connection} as created using the
#'                                \code{\link[DatabaseConnector]{connect}} function in the
#'                                DatabaseConnector package. Can be left NULL if
#'                                \code{connectionDetails} is provided, in which case a new connection
#'                                will be opened at the start of the function, and closed when the
#'                                function finishes.
#' @param resultsDatabaseSchema   (optional) The databaseSchema where the results data model of cohort
#'                                diagnostics is stored. This is only required when
#'                                \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}}
#'                                is provided.