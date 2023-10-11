test_that("Cohort subset naming and instantitation", {
  cohortSubsetNamed <- createCohortSubset(
    cohortIds = c(11, 22),
    cohortCombinationOperator = "all",
    negate = FALSE,
    startWindow = createSubsetCohortWindow(0, 90, "cohortStart"),
    endWindow = createSubsetCohortWindow(0, 50, "cohortEnd")
  )
  expectedName <- "in all of cohorts: (11, 22) starts within D: 0 - D: 90 of cohort start and ends D: 0 - D: 50 of cohort end"
  expect_equal(expectedName, cohortSubsetNamed$name)

  cohortSubsetNamed$name <- "foo"
  expect_equal("foo", cohortSubsetNamed$name)

  cohortSubsetNamed <- createCohortSubset(
    cohortIds = c(11, 22),
    cohortCombinationOperator = "any",
    negate = TRUE,
    startWindow = createSubsetCohortWindow(0, 90, "cohortStart"),
    endWindow = createSubsetCohortWindow(0, 50, "cohortEnd")
  )
  expectedName <- "not in any of cohorts: (11, 22) starts within D: 0 - D: 90 of cohort start and ends D: 0 - D: 50 of cohort end"
  expect_equal(expectedName, cohortSubsetNamed$name)
})


test_that("limit subset naming and instantitation", {
  expect_error(createLimitSubset())
  expect_error(createLimitSubset(calendarEndDate = "2001/01/01", priorTime = 300, limitTo = "pirstEmaining"))

  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 0, limitTo = "latestRemaining")
  expectedName <- "latest remaining occurence with at least 365 days prior observation"
  expect_equal(expectedName, limitSubsetNamed$name)
  limitSubsetNamed$name <- "foo"
  expect_equal("foo", limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 2, limitTo = "firstEver")
  expectedName <- "first ever occurence with at least 365 days prior observation and 2 days follow up observation"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 0, followUpTime = 200, limitTo = "lastEver")
  expectedName <- "last ever occurence with at least 200 days follow up observation"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 0, followUpTime = 200, limitTo = "earliestRemaining")
  expectedName <- "earliest remaining occurence with at least 200 days follow up observation"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(calendarStartDate = "2001/01/01")
  expectedName <- "occurs after 2001-01-01"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(calendarEndDate = "2001/01/01")
  expectedName <- "occurs before 2001-01-01"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(calendarStartDate = "2001/12/31", calendarEndDate = "2010/01/01")
  expectedName <- "occurs after 2001-12-31 and before 2010-01-01"
  expect_equal(expectedName, limitSubsetNamed$name)
})


test_that("Demographic subset naming", {
  demoSubset <- createDemographicSubset(ageMin = 32)
  expectedName <- "aged 32+"

  demoSubset <- createDemographicSubset(gender = "male", ageMin = 18, ageMax = 99)
  expectedName <- "males aged 18 - 99"
  expect_equal(expectedName, demoSubset$name)

  demoSubset <- createDemographicSubset(ageMin = 18, ageMax = 99, gender = c("foo", "MAlE", "female"), race = c(44), ethnicity = c(88))
  expectedName <- "unknown gender, males, females aged 18 - 99, race: 44, ethnicity: 88"
  expect_equal(expectedName, demoSubset$name)

  demoSubset <- createDemographicSubset(ageMin = 18, ageMax = 99, gender = c(11, 8532), race = c(44), ethnicity = c(88))
  expectedName <- "gender concept: 11, females aged 18 - 99, race: 44, ethnicity: 88"
  expect_equal(expectedName, demoSubset$name)

  demoSubset$name <- "foo"
  expect_equal("foo", demoSubset$name)
})
