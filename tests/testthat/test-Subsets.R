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
                       startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
                       endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")),
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
                                            subsetOperators = subsetOperations)

  for (s in subsetDef$subsetOperators) {
    checkmate::expect_class(s, "SubsetOperator")
  }

  listDef <- subsetDef$toList()

  checkmate::expect_list(listDef)
  expect_equal(length(subsetDef$subsetOperators), length(listDef$subsetOperators))
  checkmate::expect_character(subsetDef$toJSON())

  # Check serialized version is identical to code defined version
  subsetDef2 <- CohortSubsetDefinition$new(subsetDef$toJSON())

  checkmate::expect_class(subsetDef2, "CohortSubsetDefinition")
  expect_equal(length(subsetDef2$subsetOperators), length(subsetDef$subsetOperators))

  expect_true(subsetDef$subsetOperators[[1]]$isEqualTo(subsetDef$subsetOperators[[1]]))

  for (i in 1:length(subsetDef2$subsetOperators)) {
    item <- subsetDef2$subsetOperators[[i]]
    itemMatch <- subsetDef$subsetOperators[[i]]
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
                            startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
                            end = createSubsetCohortWindow(-99999, 99999, "cohortEnd"))
  expect_false(testDemoSubset2$isEqualTo(testDemoSubset))
})


test_that("Saving and loading definitions via attributes", {
  
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
                       startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
                       endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")),
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
                                            subsetOperators = subsetOperations)
  
  cohortDefinitionSet <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef)
  
  expect_true(hasSubsetDefinitions(cohortDefinitionSet))
  
  checkmate::expect_list(attr(cohortDefinitionSet, "cohortSubsetDefinitions"),
                         types = "CohortSubsetDefinition",
                         len = 1)
  
  savePath <- tempfile()
  unlink(savePath, recursive = T)
  on.exit(unlink(savePath, recursive = T), add = TRUE)
  saveCohortDefinitionSet(cohortDefinitionSet,
                          cohortFileNameFormat = "%s",
                          settingsFileName = file.path(savePath, "Cohorts.csv"),
                          jsonFolder = file.path(savePath, "cohorts"),
                          sqlFolder = file.path(savePath, "sql/sql_server"),
                          subsetJsonFolder = file.path(savePath, "subsetDefs"))
  checkmate::expect_directory_exists(file.path(savePath, "subsetDefs"))
  checkmate::expect_file_exists(file.path(savePath, "subsetDefs", paste0(subsetDef$definitionId, ".json")))
  
  reloadedSet <- getCohortDefinitionSet(settingsFileName = file.path(savePath, "Cohorts.csv"),
                                        jsonFolder = file.path(savePath, "cohorts"),
                                        sqlFolder = file.path(savePath, "sql/sql_server"),
                                        subsetJsonFolder = file.path(savePath, "subsetDefs"))
  expect_true(hasSubsetDefinitions(reloadedSet))
  checkmate::expect_list(attr(reloadedSet, "cohortSubsetDefinitions"), types = "CohortSubsetDefinition", min.len = 1, max.len = 1)
})



test_that("subset generation", {

  cohortDefinitionSet <- getCohortDefinitionSet(settingsFileName = "testdata/name/Cohorts.csv",
                                                jsonFolder = "testdata/name/cohorts",
                                                sqlFolder = "testdata/name/sql/sql_server",
                                                cohortFileNameFormat = "%s",
                                                cohortFileNameValue = c("cohortName"),
                                                packageName = "CohortGenerator",
                                                verbose = FALSE)
  checkmate::expect_list(getSubsetDefinitions(cohortDefinitionSet), len = 0)

  subsetOperations <- list(
    createCohortSubset(id = 1001,
                       name = "Cohort Subset",
                       cohortIds = 11,
                       cohortCombinationOperator = "all",
                       negate = FALSE,
                       startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
                       endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")),
    createDemographicSubset(id = 1003,
                            name = "Demographic Criteria",
                            ageMin = 18,
                            ageMax = 64)
  )
  subsetDef <- createCohortSubsetDefinition(name = "test definition",
                                            definitionId = 1,
                                            subsetOperators = subsetOperations)

  cohortDefinitionSetWithSubset <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef)

  checkmate::expect_list(getSubsetDefinitions(cohortDefinitionSetWithSubset), min.len = 1, types = "CohortSubsetDefinition")

  expect_true(nrow(cohortDefinitionSetWithSubset) == 6)

  # Test only applying to a subset
  cohortDefinitionSetWithSubset2 <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef, targetCohortIds = c(1, 2))

  expect_true(nrow(cohortDefinitionSetWithSubset2) == 5)

  expect_true(attr(cohortDefinitionSetWithSubset, "hasSubsetDefinitions"))
  expect_true("isSubset" %in% colnames(cohortDefinitionSetWithSubset))
  expect_true("subsetParent" %in% colnames(cohortDefinitionSetWithSubset))

  recordKeepingFolder <- tempfile("gen_subsets")
  unlink(recordKeepingFolder)
  on.exit(unlink(recordKeepingFolder), add = TRUE)
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
