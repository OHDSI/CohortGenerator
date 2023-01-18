#' @export
generateCohortSubset <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                 cdmDatabaseSchema,
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
      subsetOperator <- subsetDef$subsets[j][[1]]
      
      # Define the table names to use for the operator. 
      # -----------------------------------------------
      # The @subset_cohort_table in the SQL refers to the
      # table where the currently constructed subset cohort
      # exists. In the first iteration of this loop,
      # this table will be cohortDatabaseSchema.cohortTable
      #
      # The @target_cohort_table in the SQL refers to the
      # location where the operator will write the resulting
      # subsetted cohort
      subsetCohortTable <- ifelse(j == 1,
                                  yes = paste(cohortDatabaseSchema, cohortTable, sep = "."),
                                  no = targetCohortTable)
      targetCohortTable <- paste0("#subset_", subsetOperator$id)
      
      # TODO: Probably a nicer way to find the different subset operators
      # and constructing the SQL necessary for the subset construction
      # Execute the subset operation
      if ("CohortSubsetOperator" %in% class(subsetOperator)) {
        message("  -- Applying cohort subset operator")
        executeCohortSubsetOperator(connection = connection,
                                    tempEmulationSchema = tempEmulationSchema,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTable = cohortTable,
                                    subsetCohortTable = subsetCohortTable,
                                    targetCohortTable = targetCohortTable,
                                    cohortSubsetDefinition = subsetDef,
                                    cohortSubsetOperator = subsetOperator)
      } else if ("LimitSubsetOperator" %in% class(subsetOperator)) {
        message("  -- Applying limit subset operator")
        executeLimitSubsetOperator(connection = connection,
                                   tempEmulationSchema = tempEmulationSchema,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   subsetCohortTable = subsetCohortTable,
                                   targetCohortTable = targetCohortTable,
                                   cohortSubsetDefinition = subsetDef,
                                   cohortSubsetOperator = subsetOperator)
      } else if ("DemographicSubsetOperator" %in% class(subsetOperator)) {
        message("  -- Applying demographic subset operator")
        executeDemographicSubsetOperator(connection = connection,
                                         tempEmulationSchema = tempEmulationSchema,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         subsetCohortTable = subsetCohortTable,
                                         targetCohortTable = targetCohortTable,
                                         cohortSubsetDefinition = subsetDef,
                                         cohortSubsetOperator = subsetOperator)
      } else {
        msg <- paste0("  Subset operator class: ", paste0(class(subsetOperator),collapse=","), " not yet implemented")
        stop(msg)
      }
      
      # If there are more than 1 operator in the subset definition,
      # truncate & drop the subsetCohortTable
      # since it has been replaced by the targetCohortTable
      if (j > 1) {
        removeTempTable(connection = connection,
                        tempEmulationSchema = tempEmulationSchema,
                        cohortTable = subsetCohortTable)
      }
      
      # If this is the last iteration of the operators for this subset
      # definition, perform the final insert of the cohort subset temp table 
      # into the cohort table.
      if (j == length(subsetDef$subsets)) {
        insertCohortSubset(connection = connection,
                           tempEmulationSchema = tempEmulationSchema,
                           cohortDatabaseSchema = cohortDatabaseSchema,
                           cohortTable = cohortTable,
                           targetCohortTable = targetCohortTable,
                           cohortSubsetDefinition = subsetDef)
        
        removeTempTable(connection = connection,
                        tempEmulationSchema = tempEmulationSchema,
                        cohortTable = targetCohortTable)
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

executeCohortSubsetOperator <- function(connection,
                                        tempEmulationSchema,
                                        cohortDatabaseSchema,
                                        cohortTable,
                                        subsetCohortTable,
                                        targetCohortTable,
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
    negate = ifelse(cohortSubsetOperator$startWindow$negate == TRUE, yes = "1", no = "0"),
    start_window_anchor = ifelse(cohortSubsetOperator$startWindow$targetAnchor == "cohortStart", yes = "cohort_start_date", no = "cohort_end_date"),
    start_window_end_day = cohortSubsetOperator$startWindow$endDay,
    start_window_start_day = cohortSubsetOperator$startWindow$startDay,
    subset_cohort_table = subsetCohortTable,
    subset_length = ifelse(cohortSubsetOperator$cohortCombinationOperator == "any", yes = 1, no = length(cohortSubsetOperator$cohortIds)),
    target_cohort_table = targetCohortTable,
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

executeLimitSubsetOperator <- function(connection,
                                       tempEmulationSchema,
                                       cdmDatabaseSchema,
                                       subsetCohortTable,
                                       targetCohortTable,
                                       cohortSubsetDefinition,
                                       cohortSubsetOperator) {
  checkmate::assert_class(x = cohortSubsetOperator, classes = "LimitSubsetOperator")
  
  # Insert the cross-reference table that maps target cohort Ids
  # to subset cohort Ids and the output cohort Id
  insertTargetOuputXrefTable(connection = connection,
                             tempEmulationSchema = tempEmulationSchema,
                             targetOutcomePairs = cohortSubsetDefinition$targetOutcomePairs)
  
  # Create the SQL for performing the cohort subset operation
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortSubsetLimit.sql", package = "CohortGenerator"))
  sql <- SqlRender::render(
    sql = sql,
    calendar_end_date = ifelse(is.null(cohortSubsetOperator$calendarEndDate), yes = '0', no = '1'),
    calendar_end_date_day = ifelse(is.null(cohortSubsetOperator$calendarEndDate), yes = '', no = lubridate::day(cohortSubsetOperator$calendarEndDate)),
    calendar_end_date_month = ifelse(is.null(cohortSubsetOperator$calendarEndDate), yes = '', no = lubridate::month(cohortSubsetOperator$calendarEndDate)),
    calendar_end_date_year = ifelse(is.null(cohortSubsetOperator$calendarEndDate), yes = '', no = lubridate::year(cohortSubsetOperator$calendarEndDate)),
    calendar_start_date = ifelse(is.null(cohortSubsetOperator$calendarStartDate), yes = '0', no = '1'),
    calendar_start_date_day = ifelse(is.null(cohortSubsetOperator$calendarStartDate), yes = '', no = lubridate::day(cohortSubsetOperator$calendarStartDate)),
    calendar_start_date_month = ifelse(is.null(cohortSubsetOperator$calendarStartDate), yes = '', no = lubridate::month(cohortSubsetOperator$calendarStartDate)),
    calendar_start_date_year = ifelse(is.null(cohortSubsetOperator$calendarStartDate), yes = '', no = lubridate::year(cohortSubsetOperator$calendarEndDate)),
    cdm_database_schema = cdmDatabaseSchema,
    follow_up_time = cohortSubsetOperator$followUpTime,
    limit_to = cohortSubsetOperator$limitTo,
    prior_time = cohortSubsetOperator$priorTime,
    subset_cohort_table = subsetCohortTable,
    target_cohort_table = targetCohortTable,
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

executeDemographicSubsetOperator <- function(connection,
                                             tempEmulationSchema,
                                             cdmDatabaseSchema,
                                             subsetCohortTable,
                                             targetCohortTable,
                                             cohortSubsetDefinition,
                                             cohortSubsetOperator) {
  checkmate::assert_class(x = cohortSubsetOperator, classes = "DemographicSubsetOperator")
  
  # Insert the cross-reference table that maps target cohort Ids
  # to subset cohort Ids and the output cohort Id
  insertTargetOuputXrefTable(connection = connection,
                             tempEmulationSchema = tempEmulationSchema,
                             targetOutcomePairs = cohortSubsetDefinition$targetOutcomePairs)
  
  # Create the SQL for performing the cohort subset operation
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortSubsetDemographic.sql", package = "CohortGenerator"))
  sql <- SqlRender::render(
    sql = sql,
    age_min = cohortSubsetOperator$ageMin,
    age_max = cohortSubsetOperator$ageMax,
    cdm_database_schema = cdmDatabaseSchema,
    ethnicity = cohortSubsetOperator$ethnicity,
    gender = cohortSubsetOperator$gender,
    race = cohortSubsetOperator$race,
    subset_cohort_table = subsetCohortTable,
    target_cohort_table = targetCohortTable,
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

removeTempTable <- function(connection,
                            tempEmulationSchema,
                            cohortTable) {
  
  # Create the SQL for performing the final cohort subset insert
  sql <- "TRUNCATE TABLE @cohort_table; DROP TABLE @cohort_table;"
  sql <- SqlRender::render(
    sql = sql,
    cohort_table = cohortTable,
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

insertCohortSubset <- function(connection, 
                               tempEmulationSchema,
                               cohortDatabaseSchema,
                               cohortTable,
                               targetCohortTable,
                               cohortSubsetDefinition) {
  
  # Insert the cross-reference table that maps target cohort Ids
  # to output cohort Id
  insertTargetOuputXrefTable(connection = connection,
                             tempEmulationSchema = tempEmulationSchema,
                             targetOutcomePairs = cohortSubsetDefinition$targetOutcomePairs)
  
  # Create the SQL for performing the final cohort subset insert
  sql <- SqlRender::readSql(system.file("sql/sql_server/CohortSubsetInsert.sql", package = "CohortGenerator"))
  sql <- SqlRender::render(
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    target_cohort_table = targetCohortTable,
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