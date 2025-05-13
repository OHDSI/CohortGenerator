connection <- DatabaseConnector::connect(connectionDetails)

withr::defer({
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())

test_that("createRxNormCohortTemplateDefinition constructs a valid cohort template definition", {
  def <- createRxNormCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    rxNormTable = "rx_cohort_ref",
    cohortDatabaseSchema = "main",
    priorObservationPeriod = 365,
    requireSecondDiagnosis = TRUE
  )

  # Assertions
  expect_s3_class(def, "CohortTemplateDefinition")
  expect_equal(def$getName(), "All RxNorm ingredient exposures")

  refs <- def$getTemplateReferences()
  expect_true(all(c("cohortId", "cohortName") %in% colnames(refs)))
  expect_false(is.null(refs))
  expect_equal(nrow(refs), 3)  # Mocked to return 3 cohorts
})

test_that("createAtcCohortTemplateDefinition constructs a valid cohort template definition", {
  def <- createAtcCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    atcTable = "atc_cohort_ref",
    cohortDatabaseSchema = "main",
    mergeIngredientEras = FALSE,
    priorObservationPeriod = 180
  )

  # Assertions
  expect_s3_class(def, "CohortTemplateDefinition")
  expect_equal(def$getName(), "All ATC 4 class exposures")

  refs <- def$getTemplateReferences()
  expect_true(all(c("cohortId", "cohortName") %in% colnames(refs)))
  expect_false(is.null(refs))
  expect_equal(nrow(refs), 3)  # Mocked to return 3 cohorts

  # Check SQL args
  expect_true("merge_ingredient_eras" %in% names(def$sqlArgs))
  expect_equal(def$sqlArgs$merge_ingredient_eras, FALSE)
})


test_that("createRxNormCohortTemplateDefinition raises an error with invalid inputs", {
  expect_error(createRxNormCohortTemplateDefinition(
    connection = NULL,  # Invalid connection
    cdmDatabaseSchema = "main",
    rxNormTable = "rx_cohort_ref",
    cohortDatabaseSchema = "main",
    priorObservationPeriod = 365
  ), "Connection object is not valid")
})

test_that("createAtcCohortTemplateDefinition warns if mergeIngredientEras is invalid", {
  expect_warning(createAtcCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    atcTable = "atc_cohort_ref",
    cohortDatabaseSchema = "main",
    mergeIngredientEras = "invalid",  # Invalid value
    priorObservationPeriod = 180
  ), "mergeIngredientEras should be a logical value")
})

test_that("createSnomedCohortTemplateDefinition handles missing optional parameters gracefully", {
  def <- createSnomedCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    conditionsTable = "snomed_cohort_ref"  # Missing `priorObservationPeriod`
  )

  expect_s3_class(def, "CohortTemplateDefinition")
  expect_equal(def$sqlArgs$prior_observation_period, 365)  # Default value should be used
})