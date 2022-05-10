library(testthat)
library(CohortGenerator)

test_that("isCamelCase returns TRUE", {
  expect_true(isCamelCase("camelCaseTest"))
})

test_that("isCamelCase returns FALSE with space", {
  expect_false(isCamelCase("camel CaseTest"))
})

test_that("isCamelCase returns FALSE with underscore", {
  expect_false(isCamelCase("camel_CaseTest"))
})

test_that("isCamelCase returns FALSE with leading capital", {
  expect_false(isCamelCase("CamelCaseTest"))
})

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
