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
  cohortsForTest <- rbind(cohortsForTest,
                          data.frame(atlasId = 999,
                                     cohortId = 999,
                                     cohortName = "missing",
                                     json = NA,
                                     cohortJsonFile = NA,
                                     sql = "SELECT * FROM foo;"))
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                          settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                          jsonFolder = file.path(exportFolder, "inst/cohorts"),
                          sqlFolder = file.path(exportFolder, "inst/sql/sql_server"))
  
  # Now read the cohort definition set and expect a warning since the JSON is not
  # present for the "missing" cohort
  expect_warning(getCohortDefinitionSet(settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                                      jsonFolder = file.path(exportFolder, "inst/cohorts"),
                                      sqlFolder = file.path(exportFolder, "inst/sql/sql_server")))
  
  # Remove the json/sql to force an error when retrieving the cohort def. set
  unlink(file.path(exportFolder, "inst/sql/sql_server/missing.json"))
  unlink(file.path(exportFolder, "inst/sql/sql_server/missing.sql"))
  
  # Now read the cohort definition set
  expect_error(getCohortDefinitionSet(settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                                      jsonFolder = file.path(exportFolder, "inst/cohorts"),
                                      sqlFolder = file.path(exportFolder, "inst/sql/sql_server"),
                                      warnOnMissingJson = FALSE))

  # Cleanup  
  unlink(exportFolder, recursive = TRUE)  
})

test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package where json/sql use the cohort name", {
  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/name/settings/CohortsToCreate.csv",
                                                jsonFolder = "testdata/name/cohorts",
                                                sqlFolder = "testdata/name/sql/sql_server",
                                                packageName = "CohortGenerator",
                                                verbose = TRUE)
})

test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package where json/sql use the cohort id", {
  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/id/settings/CohortsToCreate.csv",
                                                jsonFolder = "testdata/id/cohorts",
                                                sqlFolder = "testdata/id/sql/sql_server",
                                                cohortFileNameFormat = "%s",
                                                cohortFileNameValue = c("cohortId"),
                                                packageName = "CohortGenerator")
})

# saveCohortDefinitionSet ---------
test_that("Call saveCohortDefinitionSet with empty cohortDefinitionSet", {
  expect_error(saveCohortDefinitionSet(cohortDefinitionSet = NULL))
})

test_that("Call saveCohortDefinitionSet - happy path", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                            settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                            jsonFolder = file.path(exportFolder, "inst/cohorts"),
                            sqlFolder = file.path(exportFolder, "inst/sql/sql_server"))
  # Verify the files were created
  expect_true(file.exists(file.path(exportFolder, "inst/settings/CohortsToCreate.csv")))
  expect_equal(length(list.files(file.path(exportFolder, "inst/cohorts"))), nrow(cohortsForTest))
  expect_equal(length(list.files(file.path(exportFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom SQL", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  # Add another row that contains a cohort with SQL only
  cohortsForTest <- rbind(cohortsForTest,
                          data.frame(atlasId = 999,
                                     cohortId = 999,
                                     cohortName = "Custom SQL",
                                     json = NA,
                                     cohortJsonFile = NA,
                                     sql = "SELECT * FROM foo;"))
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                          settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                          jsonFolder = file.path(exportFolder, "inst/cohorts"),
                          sqlFolder = file.path(exportFolder, "inst/sql/sql_server"))
  # Verify the files were created
  expect_true(file.exists(file.path(exportFolder, "inst/settings/CohortsToCreate.csv")))
  expect_equal(length(list.files(file.path(exportFolder, "inst/cohorts"))), nrow(cohortsForTest)-1)
  expect_equal(length(list.files(file.path(exportFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom file names", {
  exportFolder <- file.path(outputFolder, "export")
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  cohortFileNameValue <- c("cohortId", "cohortName")
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                          settingsFileName = file.path(exportFolder, "inst/settings/CohortsToCreate.csv"),
                          jsonFolder = file.path(exportFolder, "inst/cohorts"),
                          sqlFolder = file.path(exportFolder, "inst/sql/sql_server"),
                          cohortFileNameFormat = "%s-%s",
                          cohortFileNameValue = cohortFileNameValue,
                          verbose = TRUE)

  # Verify the file name is correct
  expectedFileNameRoot <- "1-celecoxib"
  expect_true(file.exists(file.path(exportFolder, "inst/cohorts/", paste0(expectedFileNameRoot, ".json"))))
  expect_true(file.exists(file.path(exportFolder, "inst/sql/sql_server", paste0(expectedFileNameRoot, ".sql"))))
  unlink(exportFolder, recursive = TRUE)
})

test_that("Call getCohortDefinitionSet with settingsFile in CohortGenerator package that is not properly formatted", {
  expect_error(getCohortDefinitionSet(settingsFileName = "testdata/invalid/settings/CohortsToCreate.csv",
                                      packageName = "CohortGenerator"))
})
