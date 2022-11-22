test_that("Subset definition", {

  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/name/Cohorts.csv",
                                                jsonFolder = "testdata/name/cohorts",
                                                sqlFolder = "testdata/name/sql/sql_server",
                                                cohortFileNameFormat = "%s",
                                                cohortFileNameValue = c("cohortName"),
                                                packageName = "CohortGenerator",
                                                verbose = TRUE)
  subsetsVec <- list(
    createCohortSubset(id = 1001,
                       name = "Cohort Subset",
                       cohortIds = 11),
    createLimitSubset(id = 1002,
                      name = "Observation Criteria",
                      priorTime = 365,
                      followUpTime = 0,
                      limitTo = "earliest"),
    createDemographicSubset(id = 1003,
                            name = "Demographic Criteria",
                            ageMin = 18,
                            ageMax = 64)
  )
  subsetDef <- createSubsetCollection(c(1, 3, 4), 11, subsetsVec)

  for (s in subsetDef$subsets) {
    checkmate::expect_class(s, "Subset")
  }

  listDef <- subsetDef$toList()

  checkmate::expect_list(listDef)
  expect_equal(length(subsetDef$subsets), length(listDef$subsets))
  checkmate::expect_character(subsetDef$toJSON())

  # Check serialized version is identical to code defined version
  subsetDef2 <- SubsetCollection$new(subsetDef$toJSON())

  checkmate::expect_class(subsetDef2, "SubsetCollection")
  expect_equal(subsetDef2$targetCohortIds, subsetDef$targetCohortIds)
  expect_equal(subsetDef$cohortId, subsetDef$cohortId)
  expect_equal(length(subsetDef2$subsets), length(subsetDef$subsets))

  for (i in 1:length(subsetDef2$subsets)) {
    item <- subsetDef2$subsets[[i]]
    itemMatch <- subsetDef$subsets[[i]]
    checkmate::expect_class(item, class(itemMatch))

    for (field in itemMatch$publicFields()) {
      if (field == "criteria") {
        expect_equal(itemMatch[[field]]$ageMax, item[[field]]$ageMax)
        expect_equal(itemMatch[[field]]$ageMin, item[[field]]$ageMin)
        expect_equal(itemMatch[[field]]$gender, item[[field]]$gender)
      } else {
        expect_equal(itemMatch[[field]], item[[field]], label = field)
      }
    }
  }
})