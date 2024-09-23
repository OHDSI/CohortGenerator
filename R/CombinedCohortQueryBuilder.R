CombinedCohortQueryBuilder <- R6::R6Class(
  classname = "CombinedCohortQueryBuilder",
  private = list(
  ),
  public = list(
    buildQuery = function(combinedCohortDefiniton) {
      checkmate::assertR6(combinedCohortDefiniton, "CombinedCohortDef")
      sql <- SqlRender::readSql(system.file("sql", "sql_server", "combinedCohorts", "unionCombine.sql", package = "CohortGenerator"))
      sql <- SqlRender::render(sql,
                               target_cohort_ids = combinedCohortDefiniton$expression$targetCohortIds,
                               warnOnMissingParameters = TRUE
      )
      return(sql)
    }
  )
)
