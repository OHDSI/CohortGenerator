test_that("Subset definition", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
      endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
    ),
    createLimitSubset(
      name = "Observation Criteria",
      priorTime = 365,
      followUpTime = 0,
      limitTo = "firstEver"
    ),
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )

  for (s in subsetDef$subsetOperators) {
    checkmate::expect_class(s, "SubsetOperator")
  }

  listDef <- subsetDef$toList()

  checkmate::expect_list(listDef)
  expect_equal(length(subsetDef$subsetOperators), length(listDef$subsetOperators))
  checkmate::expect_character(subsetDef$toJSON())
  # check reference isn't passed
  operators <- subsetDef$subsetOperators
  # Operators should not be modfiable after being added to the subset definition
  operators[[1]]$cohortIds <- 22
  expect_equal(subsetDef$subsetOperators[[1]]$cohortIds, 11)

  # Check serialized version is identical to code defined version
  subsetDef2 <- CohortSubsetDefinition$new(subsetDef$toJSON())

  expect_equal(subsetDef2$toJSON(), subsetDef$toJSON())
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

  testDemoSubset <- createDemographicSubset(
    ageMin = 18,
    ageMax = 64
  )

  expect_true(testDemoSubset$isEqualTo(testDemoSubset))

  testDemoSubset2 <- createDemographicSubset(
    gender = "nb",
    ageMin = 18,
    ageMax = 64
  )

  expect_false(testDemoSubset2$isEqualTo(testDemoSubset))

  ccs <- createCohortSubset(
    cohortIds = 11,
    cohortCombinationOperator = "all",
    negate = FALSE,
    startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
    end = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
  )
  expect_false(testDemoSubset2$isEqualTo(testDemoSubset))

  # Attempt to add an existing operator to a cohort subset definition
  csd <- createCohortSubsetDefinition(
    name = "Test cohort subset definition",
    definitionId = 1,
    subsetOperators = list(ccs)
  )

  # Create a cohort subset operator that does not reference a cohort ID
  # in the cohort definition set
  invalidCohortSubsetOperator <- createCohortSubset(
    name = "Invalid Cohort Subset",
    cohortIds = 0,
    cohortCombinationOperator = "all",
    negate = FALSE,
    startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
    end = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
  )
  invalidCohortSubsetDefintion <- createCohortSubsetDefinition(
    name = "Invalid cohort subset definition",
    definitionId = 100,
    identifierExpression = expression(targetId), # This expression will yield duplicate IDs by design
    subsetOperators = list(invalidCohortSubsetOperator)
  )

  expect_error(addCohortSubsetDefinition(
    cohortDefinitionSet = cohortDefinitionSet,
    cohortSubsetDefintion = invalidCohortSubsetDefintion
  ))

  invalidCohortSubsetOperator2 <- csd$addSubsetOperator(invalidCohortSubsetOperator)
  expect_equal(invalidCohortSubsetOperator2$toJSON(), csd$toJSON())
})


test_that("Saving and loading definitions via attributes", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
      endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
    ),
    createLimitSubset(
      priorTime = 365,
      followUpTime = 0,
      limitTo = "firstEver"
    ),
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )

  cohortDefinitionSet <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef)

  expect_true(hasSubsetDefinitions(cohortDefinitionSet))

  checkmate::expect_list(attr(cohortDefinitionSet, "cohortSubsetDefinitions"),
    types = "CohortSubsetDefinition",
    len = 1
  )

  savePath <- tempfile()
  unlink(savePath, recursive = T)
  on.exit(unlink(savePath, recursive = T), add = TRUE)
  saveCohortDefinitionSet(cohortDefinitionSet,
    cohortFileNameFormat = "%s",
    settingsFileName = file.path(savePath, "Cohorts.csv"),
    jsonFolder = file.path(savePath, "cohorts"),
    sqlFolder = file.path(savePath, "sql/sql_server"),
    subsetJsonFolder = file.path(savePath, "subsetDefs")
  )
  checkmate::expect_directory_exists(file.path(savePath, "subsetDefs"))
  checkmate::expect_file_exists(file.path(savePath, "subsetDefs", paste0(subsetDef$definitionId, ".json")))

  reloadedSet <- getCohortDefinitionSet(
    settingsFileName = file.path(savePath, "Cohorts.csv"),
    jsonFolder = file.path(savePath, "cohorts"),
    sqlFolder = file.path(savePath, "sql/sql_server"),
    subsetJsonFolder = file.path(savePath, "subsetDefs")
  )
  expect_true(hasSubsetDefinitions(reloadedSet))
  checkmate::expect_list(attr(reloadedSet, "cohortSubsetDefinitions"), types = "CohortSubsetDefinition", min.len = 1, max.len = 1)
})


test_that("subset generation", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  checkmate::expect_list(getSubsetDefinitions(cohortDefinitionSet), len = 0)

  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      startWindow = createSubsetCohortWindow(-99999, 99999, "cohortStart"),
      endWindow = createSubsetCohortWindow(-99999, 99999, "cohortEnd")
    ),
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )

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

test_that("Subset definition creation and retrieval with definitionId != 1", {
  sampleCohorts <- CohortGenerator::createEmptyCohortDefinitionSet()
  cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
  cohortJsonFileName <- cohortJsonFiles[1]
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  sampleCohorts <- rbind(sampleCohorts, data.frame(
    cohortId = as.double(1),
    cohortName = cohortName,
    json = cohortJson,
    sql = "",
    stringsAsFactors = FALSE
  ))


  # Limit to male only
  subsetDef2 <- CohortGenerator::createCohortSubsetDefinition(
    name = "Male Only",
    definitionId = 2,
    subsetOperators = list(
      CohortGenerator::createDemographicSubset(
        name = "Male",
        gender = 8507
      )
    )
  )

  sampleCohortsWithSubsets <- sampleCohorts %>%
    CohortGenerator::addCohortSubsetDefinition(subsetDef2)

  sampleSubsetDefinitions <- CohortGenerator::getSubsetDefinitions(sampleCohortsWithSubsets)
  expect_equal(length(sampleSubsetDefinitions), 1)
})

test_that("Test overwriteExisting", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  subsetOperations <- list(
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )

  # Expect to work the 1st time
  cohortDefinitionSetWithSubset <- cohortDefinitionSet %>%
    CohortGenerator::addCohortSubsetDefinition(subsetDef)

  # Expect to fail the 2nd time
  expect_error(CohortGenerator::addCohortSubsetDefinition(cohortDefinitionSetWithSubset, subsetDef))

  # Use the overwrite option
  cohortDefinitionSetWithSubset2 <- cohortDefinitionSetWithSubset %>%
    CohortGenerator::addCohortSubsetDefinition(subsetDef, overwriteExisting = TRUE)
})


test_that("Subset operator serialization tests", {
  # Confirm .loadJson fails when a non-list object is passed
  expect_error(CohortGenerator:::.loadJson(definition = 1))

  # Subset Window
  sw1 <- createSubsetCohortWindow(-99999, 99999, "cohortStart")
  sw2 <- createSubsetCohortWindow(-99999, 99999, "cohortEnd")

  expect_false(sw1$isEqualTo(sw2))
  expect_silent(sw1$toJSON())

  # SubsetOperator base class tests
  so1 <- SubsetOperator$new()
  so1$name <- "SubsetOp1"

  so2 <- SubsetOperator$new()
  so2$name <- "SubsetOp2"

  ds1 <- createDemographicSubset(
    name = "Demographic Criteria",
    ageMin = 18,
    ageMax = 64,
    gender = 8532,
    race = 8527,
    ethnicity = 38003563
  )

  expect_warning(so1$isEqualTo(ds1))
  expect_false(so1$isEqualTo(so2))
  expect_false(so2$isEqualTo(so1))
  expect_silent(so1$toJSON())
  expect_silent(so2$toJSON())
  expect_silent(ds1$toJSON())

  # Test getters
  expect_equal(ds1$getRace(), 8527)
  expect_equal(ds1$getEthnicity(), 38003563)

  ls1 <- createLimitSubset(
    name = "Limit Subset 1",
    priorTime = 365,
    followUpTime = 0,
    limitTo = "firstEver",
    calendarStartDate = "",
    calendarEndDate = ""
  )
  expect_silent(ls1$toJSON())

  ls2 <- createLimitSubset(
    name = "Limit Subset 2",
    priorTime = 365,
    followUpTime = 0,
    limitTo = "firstEver",
    calendarStartDate = "2000-01-01",
    calendarEndDate = "2013-12-31"
  )
  expect_silent(ls2$toJSON())
})

test_that("Subset name templates function", {
  cohortDefinitionSet <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  subsetOperations <- list(
    createDemographicSubset(
      name = "Demographic Criteria 1",
      ageMin = 18,
      ageMax = 64
    ),
    createDemographicSubset(
      name = "Demographic Criteria 2",
      ageMin = 32,
      ageMax = 48
    )
  )
  subsetDef <- createCohortSubsetDefinition(
    name = "test definition 123",
    definitionId = 1,
    subsetOperators = subsetOperations,
    subsetCohortNameTemplate = "FOOO @baseCohortName @subsetDefinitionName @operatorNames",
    operatorNameConcatString = "zzzz"
  )

  cohortDefinitionSetWithSubset <- cohortDefinitionSet %>%
    CohortGenerator::addCohortSubsetDefinition(subsetDef)

  # Check name templates are applied
  expect_true(all(grepl("FOOO (.+) test definition 123 Demographic Criteria 1zzzzDemographic Criteria 2", cohortDefinitionSetWithSubset$cohortName[4:6])))
})
