test_that("Subset definition", {

  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/name/Cohorts.csv",
                                                jsonFolder = "testdata/name/cohorts",
                                                sqlFolder = "testdata/name/sql/sql_server",
                                                cohortFileNameFormat = "%s",
                                                cohortFileNameValue = c("cohortName"),
                                                packageName = "CohortGenerator",
                                                verbose = FALSE)


  subsetOperations <- list(
    createCohortSubset(id = 1001,
                       name = "Cohort Subset",
                       cohortIds = 11,
                       cohortCombinationOperator = "all",
                       negate = FALSE,
                       startWindow = createSubsetCohortWindow(-999999, 99999, "cohortStart"),
                       endWindow = createSubsetCohortWindow(-999999, 99999, "cohortEnd")),
    createLimitSubset(id = 1002,
                      name = "Observation Criteria",
                      priorTime = 365,
                      followUpTime = 0,
                      limitTo = "firstEver"),
    createDemographicSubset(id = 1003,
                            name = "Demographic Criteria",
                            ageMin = 18,
                            ageMax = 64)
  )
  subsetDef <- createCohortSubsetDefinition(name = "test definition",
                                            definitionId = 1,
                                            targetOutputPairs = list(c(1, 1003), c(2, 1002)),
                                            subsets = subsetOperations)

  for (s in subsetDef$subsets) {
    checkmate::expect_class(s, "SubsetOperator")
  }

  listDef <- subsetDef$toList()

  checkmate::expect_list(listDef)
  expect_equal(length(subsetDef$subsets), length(listDef$subsets))
  checkmate::expect_character(subsetDef$toJSON())

  # Check serialized version is identical to code defined version
  subsetDef2 <- CohortSubsetDefinition$new(subsetDef$toJSON())

  checkmate::expect_class(subsetDef2, "CohortSubsetDefinition")
  expect_equal(length(subsetDef2$subsets), length(subsetDef$subsets))

  expect_true(subsetDef$subsets[[1]]$isEqualTo(subsetDef$subsets[[1]]))

  for (i in 1:length(subsetDef2$subsets)) {
    item <- subsetDef2$subsets[[i]]
    itemMatch <- subsetDef$subsets[[i]]
    checkmate::expect_class(item, class(itemMatch))
    expect_true(item$isEqualTo(itemMatch), label = paste(i, "isEqualTo"))

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

  testDemoSubset <- createDemographicSubset(id = 1003,
                                            name = "Demographic Criteria",
                                            ageMin = 18,
                                            ageMax = 64)

  expect_true(testDemoSubset$isEqualTo(testDemoSubset))

  testDemoSubset2 <- createDemographicSubset(id = 1003,
                                             name = "Demographic Criteria",
                                             gender = "nb",
                                             ageMin = 18,
                                             ageMax = 64)

  expect_false(testDemoSubset2$isEqualTo(testDemoSubset))

  ccs <- createCohortSubset(id = 1001,
                            name = "Cohort Subset",
                            cohortIds = 11,
                            cohortCombinationOperator = "all",
                            negate = FALSE,
                            startWindow = createSubsetCohortWindow(-999999, 99999, "cohortStart"),
                            end = createSubsetCohortWindow(-999999, 99999, "cohortEnd"))
  expect_false(testDemoSubset2$isEqualTo(testDemoSubset))
})


test_that("subset generation", {

  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/name/Cohorts.csv",
                                                jsonFolder = "testdata/name/cohorts",
                                                sqlFolder = "testdata/name/sql/sql_server",
                                                cohortFileNameFormat = "%s",
                                                cohortFileNameValue = c("cohortName"),
                                                packageName = "CohortGenerator",
                                                verbose = FALSE)
  subsetOperations <- list(
    createCohortSubset(id = 1001,
                       name = "Cohort Subset",
                       cohortIds = 11,
                       cohortCombinationOperator = "all",
                       negate = FALSE,
                       startWindow = createSubsetCohortWindow(-999999, 99999, "cohortStart"),
                       endWindow = createSubsetCohortWindow(-999999, 99999, "cohortEnd")),
    createDemographicSubset(id = 1003,
                            name = "Demographic Criteria",
                            ageMin = 18,
                            ageMax = 64)
  )
  subsetDef <- createCohortSubsetDefinition(name = "test definition",
                                            definitionId = 1,
                                            targetOutputPairs = list(c(1, 1003), c(2, 1002)),
                                            subsets = subsetOperations)

  cohortDefinitionSetWithSubset <- cohortDefinitionSet %>% addCohortSubsetDefinition(subsetDef)

  expect_true(attr(cohortDefinitionSetWithSubset, "hasSubsetDefinitions"))
  expect_true("isSubset" %in% colnames(cohortDefinitionSetWithSubset))
  expect_true("subsetParent" %in% colnames(cohortDefinitionSetWithSubset))

  recordKeepingFolder <- file.path(outputFolder, "RecordKeeping")
  cohortTableNames <- getCohortTableNames(cohortTable = "gen_subsets")
  createCohortTables(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )
  # 1st run first to ensure that all cohorts are generated
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSetWithSubset,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  # 2nd run using incremental mode to verify that all cohorts are created
  # but the return indicates that nothing new was generated
  cohortsGenerated <- generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSetWithSubset,
    incremental = TRUE,
    incrementalFolder = recordKeepingFolder
  )
  expect_equal(nrow(cohortsGenerated), nrow(cohortDefinitionSetWithSubset))
  expect_true(all(cohortsGenerated$generationStatus == "SKIPPED"))
  unlink(recordKeepingFolder, recursive = TRUE)
})