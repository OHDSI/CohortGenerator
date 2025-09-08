connection <- DatabaseConnector::connect(connectionDetails)

withr::defer({
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())


test_that("createSnomedCohortTemplateDefinition", {
  nameSuffix <- "(365 days po)"
  def <- createSnomedCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
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

test_that("createRxNormCohortTemplateDefinition", {
  def <- createRxNormCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main"
  )

  checkmate::expect_r6(def, "CohortTemplateDefinition")
  refs <- def$getTemplateReferences()
  checkmate::expect_data_frame(refs, nrows = 91)
  cohortDefinitionSet <- addCohortTemplateDefintion(cohortTemplateDefintion = def)

  expect_true(all(grepl("RxNorm", refs$cohortName)))

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
    databaseId = "Eunomia"
  )

  expect_false(all(count$cohortEntries == 0))
})

test_that("createAtcCohortTemplateDefinition", {

  # Required - INSERT TEST DATA INTO Concept and concept ancestor tables
  # Eunomia data does not map to atc by default
  sql <- "
  INSERT INTO concept (CONCEPT_ID, CONCEPT_NAME, DOMAIN_ID, VOCABULARY_ID, CONCEPT_CLASS_ID,
                       STANDARD_CONCEPT, CONCEPT_CODE, VALID_START_DATE, VALID_END_DATE)
  SELECT
    21603991 as concept_id, 'Coxibs' as concept_name, 'Drug' as domain_id, 'ATC' as vocabulary_id,
    'ATC 4th' as concept_class_id, 'C' as  standard_concept, 'M01AH' as concept_code,
    '1970-01-01' as valid_start_date, '2099-12-31' as valid_end_date;

  INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation,
                                max_levels_of_separation)
  SELECT 21603991 as ancestor_concept_id, 1118084 as descendant_concept_id,
         1 as min_levels_of_separation,1  as max_levels_of_separation;
  "
  DatabaseConnector::executeSql(connection, sql)
  def <- createAtcCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    nameSuffix = " (merged eras)"
  )

  def2 <- createAtcCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = "main",
    identifierExpression = "CAST(concept_id as bigint) * 1000 + 5",
    mergeIngredientEras = FALSE,
    nameSuffix = ""
  )

  checkmate::expect_r6(def, "CohortTemplateDefinition")
  refs <- def$getTemplateReferences()
  checkmate::expect_data_frame(refs, nrows = 1)
  cohortDefinitionSet <- addCohortTemplateDefintion(cohortTemplateDefintion = def) |>
    addCohortTemplateDefintion(cohortTemplateDefintion = def2)

  expect_true(all(grepl("ATC", refs$cohortName)))

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
    databaseId = "Eunomia"
  )

  expect_false(all(count$cohortEntries == 0))
})