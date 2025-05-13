connection <- DatabaseConnector::connect(connectionDetails)

withr::defer({
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())


test_that("createSnomedCohortTemplateDefinition", {
  nameSuffix <- "(365 days po)"
  def <- createSnomedCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    requireSecondDiagnosis = FALSE,
    nameSuffix = nameSuffix
  )

  checkmate::expect_r6(def, "CohortTemplateDefinition")
  refs <- def$getTemplateReferences()
  checkmate::expect_data_frame(refs, nrows = 81)
  cohortDefinitionSet <- addCohortTemplateDefintion(cohortTemplateDefintion = def)

  expect_true(all(grepl("365", refs$cohortName)))

  # Generate
  testOutputFolder <- file.path(outputFolder, "tpl_tests")
  cohortTableNames <- CohortGenerator::getCohortTableNames("cohort_tpl")

  createCohortTables(connection = connection,
                     cohortTableNames = cohortTableNames,
                     cohortDatabaseSchema = "main")

  generateCohortSet(connection = connection,
                    cdmDatabaseSchema = "main",
                    cohortDatabaseSchema = "main",
                    cohortTableNames = cohortTableNames,
                    cohortDefinitionSet = cohortDefinitionSet,
                    stopOnError = TRUE,
                    incremental = TRUE,
                    incrementalFolder = testOutputFolder)
  # check the count is consistent with expectations
  count <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = "cohort_tpl",
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = "Eunomia",
    cohortIds = c(192671000, 255848000)
  )

  expect_equal(count$cohortEntries, c(479, 52))
})