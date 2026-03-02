library(testthat)
library(CohortGenerator)

test_that("computeCohortAttrition returns attrition for each modeId", {
  cohortInclusionResult <- data.frame(
    databaseId = rep("Eunomia", 8),
    cohortDefinitionId = c(1, 1, 1, 1, 1, 1, 2, 2),
    inclusionRuleMask = c(0, 1, 3, 0, 1, 3, 0, 0),
    modeId = c(1, 1, 1, 0, 0, 0, 1, 0),
    personCount = c(100, 80, 50, 120, 90, 55, 10, 20)
  )

  cohortInclusion <- data.frame(
    cohortDefinitionId = c(1, 1, 2),
    ruleSequence = c(0L, 1L, 0L)
  )

  attrition <- computeCohortAttrition(
    cohortInclusionResult = cohortInclusionResult,
    cohortInclusion = cohortInclusion
  )

  expect_setequal(unique(attrition$modeId), c(0, 1))

  cohort1Mode1Base <- attrition$personCount[
    attrition$cohortDefinitionId == 1 &
      attrition$modeId == 1 &
      attrition$cohortEntry == 1
  ]
  expect_equal(cohort1Mode1Base, 230)

  cohort1Mode0Rule1 <- attrition$personCount[
    attrition$cohortDefinitionId == 1 &
      attrition$modeId == 0 &
      attrition$cohortEntry == 0 &
      attrition$ruleSequence == 1
  ]
  expect_equal(cohort1Mode0Rule1, 55)

  cohort2RuleRows <- attrition[
    attrition$cohortDefinitionId == 2 &
      attrition$cohortEntry == 0 &
      attrition$ruleSequence == 0,
    c("modeId", "personCount")
  ]
  expect_equal(nrow(cohort2RuleRows), 2)
  expect_setequal(cohort2RuleRows$modeId, c(0, 1))
  expect_true(all(cohort2RuleRows$personCount == 0))
})
