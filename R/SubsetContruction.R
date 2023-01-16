#' @export
generateCohortSubset <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                 cohortDatabaseSchema,
                                 cohortTable = getCohortTableNames()$cohortTable,
                                 subsetDefinitions) {
  # TODO: Input validation checks
  checkmate::assertList(x = subsetDefinitions, min.len = 1)
  
  if (is.null(connection) && is.null(connectionDetails)) {
    stop("You must provide either a database connection or the connection details.")
  }
  
  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  # Generate the subsets
  for(i in 1:length(subsetDefinitions)) {
    subsetDef <- subsetDefinitions[[i]]
    msg <- paste0("Generating subset: ", subsetDef$name, " (", i, "/", length(subsetDefinitions), ")")
    message(msg)
    for (j in 1:length(subsetDef$subsets)) {
      # TODO: Probably a nicer way to find the different subset operators
      # and constructing the SQL necessary for the subset construction
      subsetOperator <- subsetDef$subsets[j][[1]]
      if ("CohortSubsetOperator" %in% class(subsetOperator)) {
        message("  -- Constructing cohort subset")
        executeCohortSubsetSql(connection = connection,
                               tempEmulationSchema = tempEmulationSchema,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               cohortTable = cohortTable,
                               cohortSubsetDefinition = subsetDef,
                               cohortSubsetOperator = subsetOperator)
      } else {
        msg <- paste0("  Subset operator class: ", paste0(class(subsetOperator),collapse=","), " not yet implemented")
        warning(msg)
      }
    }
  }
  
  delta <- Sys.time() - start
  writeLines(paste("Generating cohort subsets took", round(delta, 2), attr(delta, "units")))
}

insertTargetOuputXrefTable <- function(connection, 
                                       tempEmulationSchema, 
                                       targetOutcomePairs, 
                                       subsetCohortIds = NULL) {
  data <- data.frame()
  
  for (i in 1:length(targetOutcomePairs)) {
    data <- rbind(data, data.frame(targetId = targetOutcomePairs[[i]][1],
                                   outputId = targetOutcomePairs[[i]][2]))
  }
  if (!is.null(subsetCohortIds)) {
    subsetCohorts <- data.frame(subsetId = subsetCohortIds)
    data <- merge(data, subsetCohorts, all = TRUE)
  } else {
    data <- cbind(data, data.frame(subsetId = NA))
  }
    
  data <- data %>%
    dplyr::arrange(targetId, subsetId, outputId) %>%
    dplyr::select(targetId, subsetId, outputId)
  
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#target_output_xref",
                                 dropTableIfExists = T,
                                 data = data,
                                 tempTable = TRUE,
                                 tempEmulationSchema = tempEmulationSchema,
                                 camelCaseToSnakeCase = TRUE)
}

executeCohortSubsetSql <- function(connection, 
                                   tempEmulationSchema,
                                   cohortDatabaseSchema,
                                   cohortTable,
                                   cohortSubsetDefinition,
                                   cohortSubsetOperator) {
  checkmate::assert_class(x = cohortSubsetOperator, classes = "CohortSubsetOperator")
  
  # Insert the cross-reference table that maps target cohort Ids
  # to subset cohort Ids and the output cohort Id
  insertTargetOuputXrefTable(connection = connection,
                             tempEmulationSchema = tempEmulationSchema,
                             targetOutcomePairs = cohortSubsetDefinition$targetOutcomePairs,
                             subsetCohortIds = cohortSubsetOperator$cohortIds)
  
  # Create the SQL for performing the cohort subset operation
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortSubsetByCohort.sql", package = "CohortGenerator"))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    end_window_anchor = ifelse(cohortSubsetOperator$endWindow$targetAnchor == "cohortStart", yes = "cohort_start_date", no = "cohort_end_date"),
    end_window_end_day = cohortSubsetOperator$endWindow$endDay,
    end_window_start_day = cohortSubsetOperator$endWindow$startDay,
    start_window_anchor = ifelse(cohortSubsetOperator$startWindow$targetAnchor == "cohortStart", yes = "cohort_start_date", no = "cohort_end_date"),
    start_window_end_day = cohortSubsetOperator$startWindow$endDay,
    start_window_start_day = cohortSubsetOperator$startWindow$startDay,
    subset_length = ifelse(cohortSubsetOperator$cohortCombinationOperator == "any", yes = 1, no = length(cohortSubsetOperator$cohortIds)),
    warnOnMissingParameters = TRUE
  )
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connection@dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  DatabaseConnector::executeSql(connection = connection,
                                sql = sql)
}