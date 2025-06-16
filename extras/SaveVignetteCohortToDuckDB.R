# Define the Cohorts Sample Data----------------------
cohortDefinitionSet <- data.frame(
  cohort_definition_id = 1,  # Match the column name in DuckDB
  cohort_name = "Celcoxib",  # Match the column name in DuckDB
  sql = "
      INSERT INTO @results_database_schema.@target_cohort_table (
          cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
      )
      SELECT 
          @target_cohort_id,
          p.person_id,
          d.start_date,
          d.end_date
      FROM 
          @cdm_database_schema.PERSON p
      CROSS JOIN (
          VALUES 
              (DATEFROMPARTS(2003, 01, 01), DATEFROMPARTS(2003, 04, 25))
      ) AS d(start_date, end_date)
     ;",  # Ensures query only produces results for person_id 1-5
  json = ""
)

cohortDefinitionSet <- rbind(
  cohortDefinitionSet,
  data.frame(
    cohort_definition_id = 2,  # Match the column name in DuckDB
    cohort_name = "Ibuprofen", # Match the column name in DuckDB
    sql = "
      INSERT INTO @results_database_schema.@target_cohort_table (
          cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
      )
      SELECT 
          @target_cohort_id,
          de.person_id,
          de.drug_exposure_start_date,
          de.drug_exposure_end_date
      FROM 
          @cdm_database_schema.drug_exposure de
      JOIN @cdm_database_schema.PERSON p ON de.person_id = p.person_id
      WHERE de.drug_exposure_start_date BETWEEN '2000-01-01' AND '2008-12-31'
      and p.person_id %2 =0;
    ",
    json = ""
  )
)
databaseFile <- tempfile(fileext = ".duckdb")
# Function to load test data into DuckDB ---------------------------
loadTestDataIntoDuckDb <- function() {
  #Setup connection
  DatabaseConnector::createConnectionDetails(
    dbms = "duckdb",
    server = databaseFile
  )
  resultsSchema <- "main"
  duckDbConnection <- DatabaseConnector::connect(duckdbConnectionDetails)
  
  # Create a table for cohort definitions
  DBI::dbExecute(duckDbConnection, "
    CREATE TABLE IF NOT EXISTS test_cohort_definitions (
      cohort_definition_id INTEGER,
      cohort_name TEXT,
      sql TEXT,
      json TEXT
    );")
  
  # Load data into DuckDB
  DBI::dbWriteTable(duckDbConnection, "test_cohort_definitions", cohortDefinitionSet, append = TRUE)
  result <- DBI::dbGetQuery(duckDbConnection, "SELECT * FROM test_cohort_definitions")
  DBI::dbDisconnect(duckDbConnection)
  return(result)
}

saveCohortResultToCSV <- function() {
  # Generate cohort result
  result <- loadTestDataIntoDuckDb()
  cohortList <- lapply(1:length(result$cohort_definition_id), function(i) {
    list(
      cohortId = result$cohort_definition_id[i],
      cohortName = result$cohort_name[i],
      sql = result$sql[i],
      json = result$json[i]
    )
  })
  
  # Convert the cohort list into a JSON formatted string with pretty printing
  jsonFormatted <- jsonlite::toJSON(cohortList, pretty = TRUE, auto_unbox = TRUE)
  jsonFilePath = "CohortGenerator/inst/testdata/CohortsToSubset.JSON"
  # Save as a JSON file
  write(jsonFormatted,jsonFilePath)
  message("Result has been saved to CohortsToSubset.JSON")
}
saveCohortResultToCSV()