library(testthat)
library(CohortGenerator)

# getCohortDefinitionSet ---------
test_that("Call getCohortDefinitionSet with missing settingsFile", {
  exportFolder <- file.path(outputFolder, "export")
  expect_error(getCohortDefinitionSet(cohortDefinitionSet = file.path(exportFolder, "CohortsToCreate.csv")))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call getCohortDefinitionSet with settingsFile that is missing cohort files", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  # Add another row that contains a cohort that lacks files on the file system
  cohortsForTest <- rbind(
    cohortsForTest,
    data.frame(
      atlasId = 999,
      cohortId = 999,
      cohortName = "missing",
      json = NA,
      cohortJsonFile = NA,
      sql = "SELECT * FROM foo;"
    )
  )
  saveCohortDefinitionSet(
    cohortDefinitionSet = cohortsForTest,
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server")
  )

  # Now read the cohort definition set and expect a warning since the JSON is not
  # present for the "missing" cohort
  expect_warning(getCohortDefinitionSet(
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server")
  ))

  # Remove the json/sql to force an error when retrieving the cohort def. set
  unlink(file.path(exportFolder, "inst/sql/sql_server/missing.json"))
  unlink(file.path(exportFolder, "inst/sql/sql_server/missing.sql"))

  # Now read the cohort definition set
  expect_error(getCohortDefinitionSet(
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server"),
    warnOnMissingJson = FALSE
  ))

  # Cleanup
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package where json/sql use the cohort name", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = TRUE
  )
  expect_equal(nrow(cohortDefinitionSet), 3)
})

test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package where json/sql use the cohort id", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/id/Cohorts.csv",
    jsonFolder = "testdata/id/cohorts",
    sqlFolder = "testdata/id/sql/sql_server",
    packageName = "CohortGenerator"
  )
  expect_equal(nrow(cohortDefinitionSet), 3)
})


test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package that is not properly formatted", {
  expect_error(getCohortDefinitionSet(
    settingsFileName = "testdata/invalid/Cohorts.csv",
    packageName = "CohortGenerator"
  ))
})

# saveCohortDefinitionSet ---------
test_that("Call saveCohortDefinitionSet with empty cohortDefinitionSet", {
  expect_error(saveCohortDefinitionSet(cohortDefinitionSet = NULL))
})

test_that("Call saveCohortDefinitionSet - happy path", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  saveCohortDefinitionSet(
    cohortDefinitionSet = cohortsForTest,
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server")
  )
  # Verify the files were created
  expect_true(file.exists(file.path(exportFolder, "inst/Cohorts.csv")))
  expect_equal(length(list.files(file.path(exportFolder, "inst/cohorts"))), nrow(cohortsForTest))
  expect_equal(length(list.files(file.path(exportFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom SQL", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  # Add another row that contains a cohort with SQL only
  cohortsForTest <- rbind(
    cohortsForTest,
    data.frame(
      atlasId = 999,
      cohortId = 999,
      cohortName = "Custom SQL",
      json = NA,
      cohortJsonFile = NA,
      sql = "SELECT * FROM foo;"
    )
  )
  saveCohortDefinitionSet(
    cohortDefinitionSet = cohortsForTest,
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server")
  )
  # Verify the files were created
  expect_true(file.exists(file.path(exportFolder, "inst/Cohorts.csv")))
  expect_equal(length(list.files(file.path(exportFolder, "inst/cohorts"))), nrow(cohortsForTest) - 1)
  expect_equal(length(list.files(file.path(exportFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom file names", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  cohortFileNameValue <- c("cohortId", "cohortName")
  saveCohortDefinitionSet(
    cohortDefinitionSet = cohortsForTest,
    settingsFileName = file.path(exportFolder, "inst/Cohorts.csv"),
    jsonFolder = file.path(exportFolder, "inst/cohorts"),
    sqlFolder = file.path(exportFolder, "inst/sql/sql_server"),
    cohortFileNameFormat = "%s-%s",
    cohortFileNameValue = cohortFileNameValue,
    verbose = TRUE
  )

  # Verify the file name is correct
  expectedFileNameRoot <- "1-celecoxib"
  expect_true(file.exists(file.path(exportFolder, "inst/cohorts/", paste0(expectedFileNameRoot, ".json"))))
  expect_true(file.exists(file.path(exportFolder, "inst/sql/sql_server", paste0(expectedFileNameRoot, ".sql"))))
  unlink(exportFolder, recursive = TRUE)
})


test_that("Call saveCohortDefinitionSet with missing json", {
  # First construct a cohort definition set: an empty
  # data frame with the cohorts to generate
  cohortsToCreate <- createEmptyCohortDefinitionSet()

  # Fill the cohort set using  cohorts included in this
  # package as an example
  cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
  for (i in 1:length(cohortJsonFiles)) {
    cohortJsonFileName <- cohortJsonFiles[i]
    cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
    cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
    cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
    cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
    cohortsToCreate <- rbind(cohortsToCreate, data.frame(
      cohortId = i,
      cohortName = cohortName,
      sql = cohortSql,
      json = cohortJson,
      stringsAsFactors = FALSE
    ))
  }

  expect_output(
    saveCohortDefinitionSet(
      cohortDefinitionSet = cohortsToCreate,
      settingsFileName = file.path(tempdir(), "settings"),
      jsonFolder = file.path(tempdir(), "json"),
      sqlFolder = file.path(tempdir(), "json")
    )
  )
})

# createEmptyCohortDefinitionSet ----------------
test_that("Call createEmptyCohortDefinitionSet in verbose mode", {
  expect_output(createEmptyCohortDefinitionSet(verbose = TRUE))
})

# isCohortDefinitionSet ----------------
test_that("Call isCohortDefinitionSet with empty cohort definition set and expect TRUE", {
  expect_true(isCohortDefinitionSet(createEmptyCohortDefinitionSet()))
})

test_that("Call isCohortDefinitionSet with real cohort definition set and expect TRUE", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/id/Cohorts.csv",
    jsonFolder = "testdata/id/cohorts",
    sqlFolder = "testdata/id/sql/sql_server",
    packageName = "CohortGenerator"
  )
  cohortDefinitionSet <- checkAndFixCohortDefinitionSetDataTypes(
    x = cohortDefinitionSet,
    fixDataTypes = TRUE,
    emitWarning = FALSE
  )$x
  expect_true(isCohortDefinitionSet(cohortDefinitionSet))
})

test_that("Call isCohortDefinitionSet with incorrect cohort definition set and expect FALSE", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/id/Cohorts.csv",
    jsonFolder = "testdata/id/cohorts",
    sqlFolder = "testdata/id/sql/sql_server",
    packageName = "CohortGenerator"
  )
  cohortDefinitionSet <- checkAndFixCohortDefinitionSetDataTypes(
    x = cohortDefinitionSet,
    fixDataTypes = TRUE,
    emitWarning = FALSE
  )$x
  cohortDefinitionSetError <- cohortDefinitionSet[, !(names(cohortDefinitionSet) %in% c("json"))]
  expect_warning(expect_false(isCohortDefinitionSet(cohortDefinitionSetError)))
})

test_that("Call isCohortDefinitionSet with cohort definition set with incorrect data type and expect FALSE", {
  cohortDefinitionSet <- createEmptyCohortDefinitionSet()
  cohortDefinitionSet$cohortName <- as.integer(cohortDefinitionSet$cohortName)
  expect_warning(expect_false(isCohortDefinitionSet(cohortDefinitionSet)))
})

# checkAndFixCohortDefinitionSetDataTypes ------------------
test_that("Call checkAndFixCohortDefinitionSetDataTypes with empty data.frame() and expect error", {
  expect_error(checkAndFixCohortDefinitionSetDataTypes(x = data.frame()))
})
