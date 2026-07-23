test_that("database test config loads PostgreSQL as the initial platform", {
  config <- getDatabaseTestConfig()

  expect_equal(config$schemaVersion, 1)
  expect_equal(config$package, "CohortGenerator")
  expect_length(config$platforms, 1)
  expect_equal(config$platforms[[1]]$dbms, "postgresql")
  expect_true(isTRUE(config$platforms[[1]]$enabled))
})

test_that("selected DBMS is read from HADES_TEST_DBMS", {
  withr::with_envvar(
    c(HADES_TEST_DBMS = "PostgreSQL"),
    {
      expect_equal(getSelectedTestDbms(), "postgresql")
      expect_equal(getDatabasePlatformConfig()$dbms, "postgresql")
    }
  )
})

test_that("missing selected DBMS returns NULL", {
  withr::with_envvar(
    c(HADES_TEST_DBMS = ""),
    {
      expect_null(getDatabasePlatformConfig())
    }
  )
})

test_that("unknown selected DBMS fails fast", {
  expect_error(
    getDatabasePlatformConfig("oracle"),
    "Database platform 'oracle' is not declared by CohortGenerator."
  )
})
