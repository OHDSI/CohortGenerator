# Define the person data--------------
omopCdmPerson <- data.frame(
  person_id = 1:12,  # Only 12 patients
  gender_concept_id = c(8507, 8507, 8532, 8532, 
                        8507, 8532, 8507, 8532, 
                        8507, 8532, 8507, 8532),  # Alternating Male (8507) and Female (8532)
  year_of_birth = c(1985, 1981, 1990, 1975, 
                    1995, 1982, 1988, 1979, 
                    1992, 1980, 1987, 1983),  # Updated years of birth for 12 patients
  race_concept_id = rep(0, 12),  # Race not specified for all
  ethnicity_concept_id = rep(0, 12) # Ethnicity not specified for all
)

# Step 2: Save the `omopCdmPerson` object into the package
usethis::use_data(omopCdmPerson,overwrite = TRUE)

# Define the drug exposure data/ Ibuprofen exposure---------
omopCdmDrugExposure <- data.frame(
  drug_exposure_id = 1:8,  # Unique IDs for drug exposures
  person_id = c(2, 4, 4, 6, 8, 10, 12, 12),  # Mapping exposures to specified subjects
  drug_concept_id = rep(2, 8),  # All exposures are Ibuprofen
  drug_exposure_start_date = c(
    as.Date("2003-01-10"),  # Subject 2: 1 exposure
    as.Date("2003-02-01"),  # Subject 4: First exposure (before celecoxib)
    as.Date("2003-11-01"),  # Subject 4: Second exposure (after celecoxib)
    as.Date("2004-01-15"),  # Subject 6: Single exposure
    as.Date("2004-02-01"),  # Subject 8: Single exposure
    as.Date("2002-03-01"),  # Subject 10: Single exposure
    as.Date("2002-05-15"),  # Subject 12: First exposure (before celecoxib)
    as.Date("2004-01-01")   # Subject 12: Second exposure (after celecoxib)
  ),
  drug_exposure_end_date = c(
    as.Date("2003-02-25"),  # Subject 2
    as.Date("2003-03-01"),  # Subject 4 (First exposure ends)
    as.Date("2004-01-15"),  # Subject 4 (Second exposure ends)
    as.Date("2004-01-30"),  # Subject 6
    as.Date("2004-03-15"),  # Subject 8
    as.Date("2002-04-15"),  # Subject 10
    as.Date("2002-07-30"),  # Subject 12 (First exposure ends)
    as.Date("2004-02-28")   # Subject 12 (Second exposure ends)
  )
)
usethis::use_data(omopCdmDrugExposure,overwrite = TRUE)

# omopCdmGIBleed <- data.frame(
#   gi_bleed_id = 1:5,  # Unique IDs for GI bleed events
#   person_id = c(4, 6, 8, 12, 12),  # Patients experiencing GI bleeds
#   gi_bleed_start_date = c(
#     as.Date("2003-02-10"),  # Subject 4: Updated to occur within 1st Ibuprofen exposure window
#     as.Date("2004-01-20"),  # Subject 6: Updated to occur within Ibuprofen exposure window
#     as.Date("2004-04-01"),  # Subject 8: Occurs after Ibuprofen exposure (unchanged)
#     as.Date("2002-06-03"),  # Subject 12: Occurs during first Ibuprofen exposure (unchanged)
#     as.Date("2004-01-10")   # Subject 12: Updated to occur within second Ibuprofen exposure window
#   ),
#   gi_bleed_end_date = c(
#     as.Date("2003-03-01"),  # Subject 4: Updated to end within 1st Ibuprofen exposure window
#     as.Date("2004-01-25"),  # Subject 6: Updated to end within Ibuprofen exposure window
#     as.Date("2004-05-15"),  # Subject 8: Ends shortly after Ibuprofen exposure (unchanged)
#     as.Date("2002-06-15"),  # Subject 12: Ends during first Ibuprofen exposure (unchanged)
#     as.Date("2004-02-20")   # Subject 12: Updated to end within second Ibuprofen exposure window
#   )
# )
# 
# usethis::use_data(omopCdmGIBleed,overwrite = TRUE)

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

cohortDefinitionSet <- rbind(
  cohortDefinitionSet,
  data.frame(
    cohort_definition_id = 3,  
    cohort_name = "GI Bleed", 
    sql = "
      INSERT INTO @results_database_schema.@target_cohort_table (
          cohort_definition_id,
          subject_id,
          cohort_start_date,
          cohort_end_date
      )
      SELECT 
          @target_cohort_id AS cohort_definition_id,
          gb.person_id AS subject_id,
          gb.gi_bleed_start_date AS cohort_start_date,
          gb.gi_bleed_end_date AS cohort_end_date
      FROM 
          @cdm_database_schema.gi_bleed gb
      JOIN 
          @cdm_database_schema.person p 
      ON 
          gb.person_id = p.person_id
      WHERE 
          gb.gi_bleed_start_date BETWEEN '2000-01-01' AND '2008-12-31';
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