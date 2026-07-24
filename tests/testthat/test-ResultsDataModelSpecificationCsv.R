test_that("resultsDataModelSpecification.csv has no parsing problems", {
  file <- system.file("csv", "resultsDataModelSpecification.csv", package = "CohortGenerator")

  dat <- readr::read_csv(
    file = file,
    col_types = readr::cols(),
    lazy = FALSE,
    progress = FALSE,
    show_col_types = FALSE
  )

  expect_equal(nrow(vroom::problems(dat)), 0)
})
