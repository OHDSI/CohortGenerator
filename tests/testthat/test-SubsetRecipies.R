test_that("addIndicationSubsetDefinition adds subset correctly for basic case", {
  # Setup initial cohortDefinitionSet
  cohortSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  
  # Call function
  result <- addIndicationSubsetDefinition(
    cohortDefinitionSet = cohortSet,
    targetCohortIds = c(1),
    indicationCohortIds = c(2),
    subsetDefinitionId = 999,
    name = "Test Indication"
  )
  
  # Check that the attribute contains the new ID
  ids <- getIndicationSubsetDefinitionIds(result)
  expect_true(999 %in% ids)
  
  # Check the attribute is properly stored
  expect_equal(attr(result, "indicationSubsetDefinitions")[[length(ids)]], 999)
  
  # Check the creation of subset operators - e.g., check your createCohortSubsetDefinition outputs
  # Can be assert-based or just check classes, etc.
})

test_that("addIndicationSubsetDefinition handles multiple cohort IDs and operator", {
  initialSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  
  res <- addIndicationSubsetDefinition(
    cohortDefinitionSet = initialSet,
    targetCohortIds = c(1),
    indicationCohortIds = c(2,3),
    subsetDefinitionId = 1000,
    cohortCombinationOperator = "all"
  )
  expect_true(1000 %in% getIndicationSubsetDefinitionIds(res))
})

test_that("addRestrictionSubsetDefinition adds restriction correctly", {
  initialSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  
  res <- addRestrictionSubsetDefinition(
    cohortDefinitionSet = initialSet,
    targetCohortIds = c(1),
    subsetDefinitionId = 2000,
    name = "Restriction Test"
  )
  
  expect_true(2000 %in% getRestrictionSubsetDefinitionIds(res))
})

test_that("addExcludeOnIndexSubsetDefinition correctly adds exclusion", {
  initialSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  
  res <- addExcludeOnIndexSubsetDefinition(
    cohortDefinitionSet = initialSet,
    name = "Exclude Test",
    targetCohortIds = c(1),
    exclusionCohortIds = c(2,3),
    exclusionWindow = 7,
    definitionId = 3000
  )

  expect_true(4000 %in% res$cohortId)
})

# Additional edge case tests:
test_that("addIndicationSubsetDefinition errors on invalid IDs", {
  cohortSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  expect_error(
    addIndicationSubsetDefinition(
      cohortDefinitionSet = cohortSet,
      targetCohortIds = c(9999),  # invalid
      indicationCohortIds = c(2),
      subsetDefinitionId = 123,
      name = "Invalid target"
    )
  )
  
  expect_error(
    addIndicationSubsetDefinition(
      cohortDefinitionSet = cohortSet,
      targetCohortIds = c(1),
      indicationCohortIds = c(9999),  # invalid
      subsetDefinitionId = 124,
      name = "Invalid indication"
    )
  )
})
