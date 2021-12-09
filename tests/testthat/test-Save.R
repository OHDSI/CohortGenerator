library(testthat)
library(CohortGenerator)

# saveCohortDefinitionSet ---------
test_that("Call saveCohortDefinitionSet with empty cohortDefinitionSet", {
  expect_error(saveCohortDefinitionSet(cohortDefinitionSet = NULL))
})

test_that("Call saveCohortDefinitionSet - happy path", {
  outputFolder <- tempdir()
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                            settingsFolder = file.path(outputFolder, "inst/settings"),
                            settingsFileName = "CohortsToCreate.csv",
                            jsonFolder = file.path(outputFolder, "inst/cohorts"),
                            sqlFolder = file.path(outputFolder, "inst/sql/sql_server"))
  # Verify the files were created
  expect_true(file.exists(file.path(outputFolder, "inst/settings/CohortsToCreate.csv")))
  expect_equal(length(list.files(file.path(outputFolder, "inst/cohorts"))), nrow(cohortsForTest))
  expect_equal(length(list.files(file.path(outputFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(outputFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom SQL", {
  outputFolder <- tempdir()
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
                            settingsFolder = file.path(outputFolder, "inst/settings"),
                            jsonFolder = file.path(outputFolder, "inst/cohorts"),
                            sqlFolder = file.path(outputFolder, "inst/sql/sql_server"))
  # Verify the files were created
  expect_true(file.exists(file.path(outputFolder, "inst/settings/CohortsToCreate.csv")))
  expect_equal(length(list.files(file.path(outputFolder, "inst/cohorts"))), nrow(cohortsForTest)-1)
  expect_equal(length(list.files(file.path(outputFolder, "inst/sql/sql_server"))), nrow(cohortsForTest))
  unlink(outputFolder, recursive = TRUE)
})

test_that("Call saveCohortDefinitionSet - custom file names", {
  outputFolder <- tempdir()
  cohortsForTest <- getCohortsForTest(cohorts = cohorts)
  cohortFileNameValue <- c("cohortId", "cohortName")
  saveCohortDefinitionSet(cohortDefinitionSet = cohortsForTest,
                            settingsFolder = file.path(outputFolder, "inst/settings"),
                            jsonFolder = file.path(outputFolder, "inst/cohorts"),
                            sqlFolder = file.path(outputFolder, "inst/sql/sql_server"),
                            cohortFileNameFormat = "%s-%s",
                            cohortFileNameValue = cohortFileNameValue)
  
  # Verify the file name is correct
  expectedFileNameRoot <- "1-celecoxib"
  expect_true(file.exists(file.path(outputFolder, "inst/cohorts/", paste0(expectedFileNameRoot, ".json"))))
  expect_true(file.exists(file.path(outputFolder, "inst/sql/sql_server", paste0(expectedFileNameRoot, ".sql"))))
  unlink(outputFolder, recursive = TRUE)
})