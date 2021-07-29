#' Load, render, and translate a SQL file in this package.
#'
#' @description
#' This helper function is used in place of 
#' using \code{SqlRender::loadRenderTranslateSql}
#' otherwise unit tests will not function properly. 
#' 
#' NOTE: This function does not support dialect-specific SQL translation
#' at this time.
#' 
#' @param sqlFilename               The source SQL file
#' @param dbms                      The target dialect. Currently 'sql server', 'oracle', 'postgres',
#'                                  and 'redshift' are supported
#' @param ...                       Parameter values used for \code{render}
#' @param tempEmulationSchema       Some database platforms like Oracle and Impala do not truly support
#'                                  temp tables. To emulate temp tables, provide a schema with write
#'                                  privileges where temp tables can be created.
#' @param warnOnMissingParameters   Should a warning be raised when parameters provided to this
#'                                  function do not appear in the parameterized SQL that is being
#'                                  rendered? By default, this is TRUE.
#'
#' @return
#' Returns a string containing the rendered SQL.
loadRenderTranslateSql <- function(sqlFilename,
                                   dbms = "sql server",
                                   ...,
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                   warnOnMissingParameters = TRUE) {
  pathToSql <- system.file(paste("sql/sql_server"),
                           sqlFilename,
                           package = "CohortGenerator",
                           mustWork = TRUE)
  sql <- SqlRender::readSql(pathToSql)
  renderedSql <- SqlRender::render(sql = sql,
                                   warnOnMissingParameters = warnOnMissingParameters,
                                   ...)
  renderedSql <- SqlRender::translate(sql = renderedSql,
                                      targetDialect = dbms,
                                      tempEmulationSchema = tempEmulationSchema)
  return(renderedSql)
}
