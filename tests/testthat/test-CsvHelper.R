library(testthat)
library(CohortGenerator)

# isSnakeCase Tests ---------------
test_that("isSnakeCase returns TRUE with expected case", {
  expect_true(isSnakeCase("snake_case_test"))
})

test_that("isSnakeCase returns TRUE when input is a single word", {
  expect_true(isSnakeCase("snake"))
})

test_that("isSnakeCase returns TRUE when input contains an integer value", {
  expect_true(isSnakeCase("snake2"))
})

test_that("isSnakeCase returns FALSE with space", {
  expect_false(isSnakeCase("snake_ case_test"))
})

test_that("isSnakeCase returns FALSE with double underscore", {
  expect_false(isSnakeCase("snake__case_test"))
})

test_that("isSnakeCase returns FALSE with upper case character", {
  expect_false(isSnakeCase("snake_Case_test"))
})

# isCamelCase Tests ----------------
test_that("isCamelCase returns TRUE with expected case", {
  expect_true(isCamelCase("camelCaseTest"))
})

test_that("isCamelCase returns TRUE with single word", {
  expect_true(isCamelCase("camel"))
})

test_that("isCamelCase returns FALSE with space", {
  expect_false(isCamelCase("camel Case"))
})

test_that("isCamelCase returns FALSE with upper case to start the string", {
  expect_false(isCamelCase("CamelCase"))
})

test_that("isCamelCase returns FALSE with underscore", {
  expect_false(isCamelCase("camel_Case"))
})

# isFormattedForDatabaseUpload Tests ---------------
test_that("isFormattedForDatabaseUpload returns TRUE with properly formatted data frame", {
  df <- createEmptyCohortDefinitionSet()
  names(df) <- SqlRender::camelCaseToSnakeCase(names(df))
  expect_true(isFormattedForDatabaseUpload(df, warn = TRUE))
})

test_that("isFormattedForDatabaseUpload returns FALSE with improperly formatted data frame", {
  df <- createEmptyCohortDefinitionSet()
  expect_false(isFormattedForDatabaseUpload(df, warn = FALSE))
})

test_that("isFormattedForDatabaseUpload provides warning with improperly formatted data frame", {
  df <- createEmptyCohortDefinitionSet()
  expect_warning(isFormattedForDatabaseUpload(df, warn = TRUE))
})

# readCsv Tests ---------------
test_that("readCsv reads a file without column casing warning", {
  testfile <- tempfile(fileext = ".csv")
  df <- createEmptyCohortDefinitionSet()
  names(df) <- SqlRender::camelCaseToSnakeCase(names(df))
  readr::write_csv(x = df, file = testfile)
  expect_invisible(readCsv(file = testfile, warnOnCaseMismatch = TRUE))
  unlink(testfile)
})

test_that("readCsv reads a file with incorrect column casing and raises a warning", {
  testfile <- tempfile(fileext = ".csv")
  df <- createEmptyCohortDefinitionSet()
  readr::write_csv(x = df, file = testfile)
  expect_warning(readCsv(file = testfile, warnOnCaseMismatch = TRUE))
  unlink(testfile)
})

# writeCsv Tests ---------------
test_that("writeCsv writes a file without column casing warning", {
  testfile <- tempfile(fileext = ".csv")
  df <- createEmptyCohortDefinitionSet()
  expect_invisible(writeCsv(
    x = df,
    file = testfile,
    warnOnCaseMismatch = TRUE,
    warnOnUploadRuleViolations = FALSE
  ))
  unlink(testfile)
})

test_that("writeCsv writes a file with column casing warning", {
  testfile <- tempfile(fileext = ".csv")
  df <- createEmptyCohortDefinitionSet()
  names(df) <- SqlRender::camelCaseToSnakeCase(names(df))
  expect_warning(writeCsv(
    x = df,
    file = testfile,
    warnOnCaseMismatch = TRUE,
    warnOnUploadRuleViolations = FALSE
  ))
  unlink(testfile)
})

test_that("writeCsv writes a file with a warning on upload rule violations", {
  testfile <- tempfile(pattern = "camelCase", fileext = ".csv")
  df <- createEmptyCohortDefinitionSet()
  expect_warning(writeCsv(
    x = df,
    file = testfile,
    warnOnCaseMismatch = FALSE,
    warnOnUploadRuleViolations = TRUE
  ))
  unlink(testfile)
})
