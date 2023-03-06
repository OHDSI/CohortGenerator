test_that("Cohort subset naming and instantitation", {
  cohortSubsetNamed <- createCohortSubset(cohortIds = c(11, 22),
                                          cohortCombinationOperator = "all",
                                          negate = FALSE,
                                          startWindow = createSubsetCohortWindow(0, 90, "cohortStart"),
                                          endWindow = createSubsetCohortWindow(0, 50, "cohortEnd"))
  expectedName <- "Coh: {subjects in all of cohorts: (11, 22) Starting D: 0 and D: 90 of target cohort start and Ending D: 0 and D: 50 of target cohort end}"
  expect_equal(expectedName, cohortSubsetNamed$name)

  cohortSubsetNamed$name <- "foo"
  expect_equal("foo", cohortSubsetNamed$name)

  cohortSubsetNamed <- createCohortSubset(cohortIds = c(11, 22),
                                          cohortCombinationOperator = "any",
                                          negate = TRUE,
                                          startWindow = createSubsetCohortWindow(0, 90, "cohortStart"),
                                          endWindow = createSubsetCohortWindow(0, 50, "cohortEnd"))
  expectedName <- "Coh: {subjects not in any of cohorts: (11, 22) Starting D: 0 and D: 90 of target cohort start and Ending D: 0 and D: 50 of target cohort end}"
  expect_equal(expectedName, cohortSubsetNamed$name)
})


test_that("limit subset naming and instantitation", {
  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 0, limitTo = "latestRemaining")
  expectedName <- "Limit to: {latest remaining occurence with at least 365 days prior observation}"
  expect_equal(expectedName, limitSubsetNamed$name)
  limitSubsetNamed$name <- "foo"
  expect_equal("foo", limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 2, limitTo = "firstEver")
  expectedName <- "Limit to: {first ever occurence with at least 365 days prior observation and 2 days follow up observation}"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 0, followUpTime = 200, limitTo = "lastEver")
  expectedName <- "Limit to: {last ever occurence with at least 200 days follow up observation}"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 0, followUpTime = 200, limitTo = "earliestRemaining")
  expectedName <- "Limit to: {earliest remaining occurence with at least 200 days follow up observation}"
  expect_equal(expectedName, limitSubsetNamed$name)

  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 0, limitTo = "", calendarStartDate = "2001/01/01")
  expectedName <- "Limit to: {subjects with at least 365 days prior observation after 2001-01-01}"
  expect_equal(expectedName, limitSubsetNamed$name)
  
  
  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 0, limitTo = "", calendarEndDate = "2001/01/01")
  expectedName <- "Limit to: {subjects with at least 365 days prior observation before 2001-01-01}"
  expect_equal(expectedName, limitSubsetNamed$name)
  
  limitSubsetNamed <- createLimitSubset(priorTime = 365, followUpTime = 0, limitTo = "", calendarStartDate = "2001/12/31", calendarEndDate = "2010/01/01")
  expectedName <- "Limit to: {subjects with at least 365 days prior observation after 2001-12-31 and before 2010-01-01}"
  expect_equal(expectedName, limitSubsetNamed$name)
})


test_that("Demographic subset naming", {
  demoSubset <- createDemographicSubset(ageMin = 18, ageMax = 99)
  expectedName <- "Demo: {age >= 18 and age <= 99}"
  expect_equal(expectedName, demoSubset$name)
  
  demoSubset <- createDemographicSubset(ageMin = 18, ageMax = 99, gender = c("foo", "MAlE", "female"), race = c(44), ethnicity = c(88))
  expectedName <- "Demo: {age >= 18 and age <= 99 and Gender concept 0, 8507, 8532 and Race concept 44 and Ethnicity concept 88}"
  expect_equal(expectedName, demoSubset$name)
  
  demoSubset <- createDemographicSubset(ageMin = 18, ageMax = 99, gender = c(11, 8532), race = c(44), ethnicity = c(88))
  expectedName <- "Demo: {age >= 18 and age <= 99 and Gender concept 11, 8532 and Race concept 44 and Ethnicity concept 88}"
  expect_equal(expectedName, demoSubset$name)
  
  demoSubset$name <- "foo"
  expect_equal("foo", demoSubset$name)
})