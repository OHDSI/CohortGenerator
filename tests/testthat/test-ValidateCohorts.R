test_that("Validate cohorts functions as intended", {
  tConnectionDetails <- DatabaseConnector::createConnectionDetails("sqlite", server = ":memory:")
  tconnection <- DatabaseConnector::connect(tConnectionDetails)

  testData <- "

-- Create the cohort table
CREATE TABLE cohort (
  cohort_definition_id BIGINT,
  subject_id BIGINT,
  cohort_start_date DATE,
  cohort_end_date DATE
);

-- Create the observation_period table
CREATE TABLE observation_period (
  person_id BIGINT,
  observation_period_start_date DATE,
  observation_period_end_date DATE
);

-- Insert test cases into cohort table

-- 1. Overlapping eras: Same cohort_definition_id, subjects id, overlapping dates
INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
VALUES
(1, 101, '2025-01-01', '2025-01-31'),
(1, 101, '2025-01-15', '2025-01-16');

-- 2. Start date after end date
INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
VALUES
(2, 201, '2025-03-01', '2025-02-28'), -- Start date is after end date
(2, 202, NULL, '2025-02-28'); -- NULL start date

-- 3. Duplicate entries
INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
VALUES
(3, 301, '2025-04-01', '2025-04-30'), -- Original
(3, 301, '2025-04-01', '2025-04-30'); -- Duplicate

-- 4. Outside observation period: Dates fall outside the observation period for a subject
INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
VALUES
(4, 401, '2025-05-01', '2025-05-31'); -- Outside observation period

-- 5. Valid cohort entry
INSERT INTO cohort (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
VALUES
(5, 501, '2025-06-01', '2025-06-30'); -- Completely valid entry

-- Insert data into observation_period table

-- Observation periods for the subjects
INSERT INTO observation_period (person_id, observation_period_start_date, observation_period_end_date)
VALUES
(101, '2025-01-01', '2025-12-31'), -- Subject 101 (valid era for cohort 1)
(301, '2001-01-01', '2025-12-31'), -- Subject 101 (valid era for cohort 1)
(401, '2025-06-01', '2025-12-31'), -- Subject 401 (invalid for cohort 4)
(501, '2025-05-01', '2025-07-31'); -- Subject 501 (valid for cohort 5)

  "

  DatabaseConnector::executeSql(connection = tconnection, sql = testData)
  validationCounts <- getCohortValidationCounts(connection = tconnection, cdmDatabaseSchema = "main", cohortDatabaseSchema = "main")
  checkmate::expect_data_frame(validationCounts)


  expect_equal(
    validationCounts |> dplyr::filter(.data$cohortDefinitionId == 1) |> dplyr::pull("overlappingErasCount"), 1
  )

  expect_equal(
    validationCounts |> dplyr::filter(.data$cohortDefinitionId == 2) |> dplyr::pull("invalidDateCount"),2
  )
  expect_equal(
    validationCounts |> dplyr::filter(.data$cohortDefinitionId == 3) |> dplyr::pull("duplicateCount"), 1
  )

  expect_equal(
    validationCounts |> dplyr::filter(.data$cohortDefinitionId == 4) |> dplyr::pull("outsideObservationCount"), 1
  )
  
  expect_false(
    all(validationCounts |> dplyr::filter(.data$cohortDefinitionId != 5) |> dplyr::pull("valid"))
  )

  expect_true(
    validationCounts |> dplyr::filter(.data$cohortDefinitionId == 5) |> dplyr::pull("valid")
  )
})