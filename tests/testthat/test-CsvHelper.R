library(testthat)
library(CohortGenerator)

# isSnakeCase Tests ---------------
test_that("isSnakeCase returns TRUE", {
  expect_true(isSnakeCase("snake_case_test"))
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

