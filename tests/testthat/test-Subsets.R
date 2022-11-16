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
                       cohortJson = cohortDefinitionSet$json[1]),
    createObservationCriteriaSubset(id = 1002,
                                    name = "Observation Criteria",
                                    priorTime = 365,
                                    followUpTime = 0,
                                    first = FALSE,
                                    last = FALSE,
                                    random = FALSE),
    createDemographicCriteriaSubset(id = 1003,
                                    name = "Demographic Criteria",
                                    ageMin = 18,
                                    ageMax = 64)
  )
  subsetDef <- createSubsetDefinition(c(1, 3, 4), subsetsVec)
  listDef <- subsetDef$toList()

  checkmate::expect_list(listDef)
  checkmate::expect_character(subsetDef$toJSON())
})