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
  windowSubsetOperation <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortEnd"
    )
  )
  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      windows = windowSubsetOperation
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

  ccsWindow <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortEnd"
    )
  )
  ccs <- createCohortSubset(
    cohortIds = 11,
    cohortCombinationOperator = "all",
    negate = FALSE,
    windows = ccsWindow
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
  invalidCohortWindow <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortEnd"
    )
  )
  invalidCohortSubsetOperator <- createCohortSubset(
    name = "Invalid Cohort Subset",
    cohortIds = 0,
    cohortCombinationOperator = "all",
    negate = FALSE,
    windows = invalidCohortWindow
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
  subsetOperationsWindow <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortEnd"
    )
  )
  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      windows = subsetOperationsWindow
    ),
    createLimitSubset(
      priorTime = 365,
      followUpTime = 0,
      minimumCohortDuration = 1,
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

  subsetOperationsWindowLogic <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 99999,
      targetAnchor = "cohortEnd"
    )
  )
  subsetOperations <- list(
    createCohortSubset(
      name = "Cohort Subset",
      cohortIds = 11,
      cohortCombinationOperator = "all",
      negate = FALSE,
      windows = subsetOperationsWindowLogic
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

  expect_true(nrow(cohortDefinitionSetWithSubset) == 8)

  # Test only applying to a subset
  cohortDefinitionSetWithSubset2 <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef, targetCohortIds = c(1, 2))

  expect_true(nrow(cohortDefinitionSetWithSubset2) == 6)

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
  expect_true(all(grepl("FOOO (.+) test definition 123 Demographic Criteria 1zzzzDemographic Criteria 2", cohortDefinitionSetWithSubset$cohortName[5:8])))

  # Internal copy call
  cds2 <- .copySubsetDefinitions(cohortDefinitionSet, cohortDefinitionSetWithSubset)

  checkmate::expect_list(attr(cds2, "cohortSubsetDefinitions"))
  expect_true(attr(cds2, "hasSubsetDefinitions"))
})

test_that("Basic Negate logic check", {
  # Testing if Negate is found in json structure
  window1 <- createSubsetCohortWindow(
    startDay = 1,
    endDay = 10,
    targetAnchor = "cohortStart",
    subsetAnchor = "cohortStart",
    negate = TRUE
  )
  expect_true(window1$negate)

  jsonOutput <- window1$toJSON()
  expect_true(grepl('"negate": true', jsonOutput))



  # Testing if Negate (AND NOT) IS FOUND IN SQL QUERY
  # What this test does is check if using a cohort celcoxib,
  # create a subset based on a year after celcoxib exposure of patients NOT exposed in the specified time window

  jsonFilePath <- system.file("testdata", "SubsetVignetteCohorts.JSON", package = "CohortGenerator")
  cohortDefinitionSet <- jsonlite::fromJSON(jsonFilePath)

  windows <- list(
    CohortGenerator::createSubsetCohortWindow(
      startDay = 1,
      endDay = 365,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortStart"
    ),
    CohortGenerator::createSubsetCohortWindow(
      startDay = 366,
      endDay = 99999,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortStart",
      negate = TRUE
    )
  )

  ibuprofenYearAfter <- CohortGenerator::createCohortSubsetDefinition(
    name = "requiring",
    definitionId = 6,
    subsetOperators = list(
      CohortGenerator::createLimitSubset(name = "first exposure", limitTo = "firstEver"),
      CohortGenerator::createCohortSubset(
        name = "with ibuprofen after a year",
        cohortIds = 3,
        cohortCombinationOperator = "any",
        negate = FALSE,
        windows = windows
      )
    )
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    CohortGenerator::addCohortSubsetDefinition(ibuprofenYearAfter, targetCohortIds = c(1))

  sqlForCohort1006 <- cohortDefinitionSet[cohortDefinitionSet$cohortId == 1006, "sql"]
  expect_true(grepl("AND NOT", sqlForCohort1006, ignore.case = TRUE))
})



test_that("Subset logic checks", {
  databaseFile <- tempfile(fileext = ".sqlite")
  sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = databaseFile
  )
  sqliteResultsDatabaseSchema <- "main"
  connection <- DatabaseConnector::connect(sqliteConnectionDetails)
  withr::defer(
    {
      DatabaseConnector::disconnect(connection)
      unlink(databaseFile, force = TRUE)
    },
    testthat::teardown_env()
  )

  # Create dummy OMOP data for testing ------------------
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = sqliteResultsDatabaseSchema,
    tableName = "observation_period",
    data = data.frame(
      observation_period_id = 1,
      person_id = 1,
      observation_period_start_date = lubridate::date("2000-01-01"),
      observation_period_end_date = lubridate::date("2008-12-31")
    )
  )

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = sqliteResultsDatabaseSchema,
    tableName = "person",
    data = data.frame(
      person_id = 1,
      gender_concept_id = 8532,
      year_of_birth = 2000,
      race_concept_id = 0,
      ethnicity_concept_id = 0
    )
  )


  # Define limit subsets for tests -------------
  lsd1 <- createCohortSubsetDefinition(
    name = "first ever",
    definitionId = 101,
    subsetOperators = list(
      createLimitSubset(
        name = "first ever",
        limitTo = "firstEver"
      )
    )
  )

  lsd2 <- createCohortSubsetDefinition(
    name = "earliestRemaining",
    definitionId = 102,
    subsetOperators = list(
      createLimitSubset(
        name = "earliestRemaining",
        limitTo = "earliestRemaining",
        priorTime = 500
      )
    )
  )

  lsd3 <- createCohortSubsetDefinition(
    name = "latestRemaining",
    definitionId = 103,
    subsetOperators = list(
      createLimitSubset(
        name = "latestRemaining",
        limitTo = "latestRemaining",
        followUpTime = 800
      )
    )
  )

  lsd4 <- createCohortSubsetDefinition(
    name = "lastEver",
    definitionId = 104,
    subsetOperators = list(
      createLimitSubset(
        name = "lastEver",
        limitTo = "lastEver"
      )
    )
  )

  lsd5 <- createCohortSubsetDefinition(
    name = "calendar",
    definitionId = 105,
    subsetOperators = list(
      createLimitSubset(
        name = "2003 - 2006",
        calendarStartDate = "2003-01-01",
        calendarEndDate = "2006-12-31",
      )
    )
  )

  lsd6 <- createCohortSubsetDefinition(
    name = "firstEver + calendar",
    definitionId = 106,
    subsetOperators = list(
      createLimitSubset(
        limitTo = "firstEver",
        name = "2003 - 2006",
        calendarStartDate = "2003-01-01",
        calendarEndDate = "2006-12-31",
      )
    )
  )

  lsd7 <- createCohortSubsetDefinition(
    name = "earliestRemaining + calendar",
    definitionId = 107,
    subsetOperators = list(
      createLimitSubset(
        limitTo = "earliestRemaining",
        name = "2003 - 2006",
        priorTime = 500,
        calendarStartDate = "2003-01-01",
        calendarEndDate = "2006-12-31",
      )
    )
  )

  # Define demographics subsets for tests -------------
  ds1 <- createCohortSubsetDefinition(
    name = "Age subset",
    definition = 201,
    subsetOperators = list(
      createDemographicSubset(
        name = "Age 2-5",
        ageMin = 2,
        ageMax = 5
      )
    )
  )

  ds2 <- createCohortSubsetDefinition(
    name = "Gender subset",
    definition = 202,
    subsetOperators = list(
      createDemographicSubset(
        name = "Gender = 8532",
        gender = 8532
      )
    )
  )

  ds3 <- createCohortSubsetDefinition(
    name = "Race subset",
    definition = 203,
    subsetOperators = list(
      createDemographicSubset(
        name = "Race = 0",
        race = 0
      )
    )
  )

  ds4 <- createCohortSubsetDefinition(
    name = "Race subset",
    definition = 204,
    subsetOperators = list(
      createDemographicSubset(
        name = "Ethnicity = 0",
        ethnicity = 0
      )
    )
  )

  # Define cohort subsets for tests -------------

  cs1Window <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 0,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = 0,
      endDay = 99999,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortEnd"
    )
  )

  cs1 <- createCohortSubsetDefinition(
    name = "Subset overlaps cohort start",
    definition = 301,
    subsetOperators = list(
      createCohortSubset(
        name = "subsetOverlapTargetStart",
        cohortIds = c(2),
        negate = F,
        cohortCombinationOperator = "all",
        windows = cs1Window
      )
    )
  )

  cs2Window <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = -1,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = 1,
      endDay = 99999,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortEnd"
    )
  )
  cs2 <- createCohortSubsetDefinition(
    name = "Subset overlaps entire target cohort period",
    definition = 302,
    subsetOperators = list(
      createCohortSubset(
        name = "subsetSubsumesTarget",
        cohortIds = c(3),
        negate = F,
        cohortCombinationOperator = "any",
        windows = cs2Window
      )
    )
  )

  cs3Windows <- list(
    createSubsetCohortWindow(
      startDay = 1,
      endDay = 99999,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 1,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortEnd"
    )
  )

  cs3 <- createCohortSubsetDefinition(
    name = "Subset subsumed by entire target cohort period",
    definition = 303,
    subsetOperators = list(
      createCohortSubset(
        name = "targetSubsumesSubset",
        cohortIds = c(4),
        negate = F,
        cohortCombinationOperator = "any",
        windows = cs3Windows
      )
    )
  )

  cs4Windows <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 0,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = 0,
      endDay = 99999,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortEnd"
    )
  )
  cs4 <- createCohortSubsetDefinition(
    name = "Subset overlaps cohort end",
    definition = 304,
    subsetOperators = list(
      createCohortSubset(
        name = "subsetOverlapTargetEnd",
        cohortIds = c(5),
        negate = F,
        cohortCombinationOperator = "any",
        windows = cs4Windows
      )
    )
  )

  cs5Windows <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 0,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = 0,
      endDay = 99999,
      targetAnchor = "cohortEnd",
      subsetAnchor = "cohortEnd"
    )
  )
  cs5 <- createCohortSubsetDefinition(
    name = "Subset does NOT overlap cohort end - negate",
    definition = 305,
    subsetOperators = list(
      createCohortSubset(
        name = "subsetOverlapTargetEndNegate",
        cohortIds = c(5),
        negate = T,
        cohortCombinationOperator = "any",
        windows = cs5Windows
      )
    )
  )

  cs6Windows <- list(
    createSubsetCohortWindow(
      startDay = -99999,
      endDay = 0,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortStart"
    ),
    createSubsetCohortWindow(
      startDay = 0,
      endDay = 99999,
      targetAnchor = "cohortStart",
      subsetAnchor = "cohortEnd"
    )
  )
  cs6 <- createCohortSubsetDefinition(
    name = "Subset overlaps target start - tests combo == all",
    definition = 306,
    subsetOperators = list(
      createCohortSubset(
        name = "subsetOverlapTargetStartComboAll",
        cohortIds = c(2, 3),
        negate = F,
        cohortCombinationOperator = "all",
        windows = cs6Windows
      )
    )
  )

  # Create cohort def. set and apply subset definitions ---------
  cohortDefinitionSet <- data.frame(
    cohortId = 1,
    cohortName = "Test Target Cohort",
    sql = "
  INSERT INTO @results_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
  )
  SELECT @target_cohort_id, 1, DATEFROMPARTS(2001, 01, 01), DATEFROMPARTS(2002, 01, 01)
  UNION
  SELECT @target_cohort_id, 1, DATEFROMPARTS(2003, 01, 01), DATEFROMPARTS(2004, 01, 01)
  UNION
  SELECT @target_cohort_id, 1, DATEFROMPARTS(2005, 01, 01), DATEFROMPARTS(2006, 01, 01)
  UNION
  SELECT @target_cohort_id, 1, DATEFROMPARTS(2007, 01, 01), DATEFROMPARTS(2008, 01, 01)
  ;",
    json = ""
  )
  cohortDefinitionSet <- rbind(
    cohortDefinitionSet,
    data.frame(
      cohortId = 2,
      cohortName = "Test Subset 1 - Subset Overlaps Target Start Date",
      sql = "
    INSERT INTO @results_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
    )
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2000, 01, 01), DATEFROMPARTS(2001, 12, 31)
    UNION
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2002, 01, 01), DATEFROMPARTS(2003, 12, 31)
    UNION
    -- NOTE: DOES NOT OVERLAP COHORT ID = 1 FOR TESTING
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2004, 01, 01), DATEFROMPARTS(2004, 12, 31)
  ;",
      json = ""
    )
  )

  cohortDefinitionSet <- rbind(
    cohortDefinitionSet,
    data.frame(
      cohortId = 3,
      cohortName = "Test Subset 2 - Subset start+end subsumes target start+end",
      sql = "
    INSERT INTO @results_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
    )
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2000, 01, 01), DATEFROMPARTS(2003, 12, 31)
    UNION
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2002, 01, 01), DATEFROMPARTS(2005, 12, 31)
    UNION
    -- NOTE: DOES NOT FULLY SUBSUME COHORT ID = 1 FOR TESTING
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2004, 01, 01), DATEFROMPARTS(2005, 12, 31)
  ;",
      json = ""
    )
  )

  cohortDefinitionSet <- rbind(
    cohortDefinitionSet,
    data.frame(
      cohortId = 4,
      cohortName = "Test Subset 3 - Target start+end subsumes Subset start+end",
      sql = "
    INSERT INTO @results_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
    )
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2001, 02, 01), DATEFROMPARTS(2001, 12, 31)
    UNION
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2003, 02, 01), DATEFROMPARTS(2003, 12, 31)
    UNION
    -- NOTE: IS NOT FULLY SUBSUMED BY COHORT ID = 1 FOR TESTING
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2004, 01, 01), DATEFROMPARTS(2005, 12, 31)
  ;",
      json = ""
    )
  )

  cohortDefinitionSet <- rbind(
    cohortDefinitionSet,
    data.frame(
      cohortId = 5,
      cohortName = "Test Subset 4 - Subset Overlaps Target End Date",
      sql = "
    INSERT INTO @results_database_schema.@target_cohort_table (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
    )
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2001, 02, 01), DATEFROMPARTS(2002, 02, 01)
    UNION
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2003, 02, 01), DATEFROMPARTS(2004, 02, 01)
    UNION
    -- NOTE: DOES NOT OVERLAP ANY END DATE ENTRIES IN COHORT ID = 1 FOR TESTING
    SELECT @target_cohort_id, 1, DATEFROMPARTS(2003, 02, 01), DATEFROMPARTS(2003, 03, 01)
  ;",
      json = ""
    )
  )

  cohortDefinitionSet <- cohortDefinitionSet |>
    addCohortSubsetDefinition(lsd1, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd2, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd3, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd4, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd5, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd6, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(lsd7, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(ds1, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(ds2, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(ds3, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(ds4, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs1, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs2, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs3, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs4, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs5, targetCohortIds = c(1)) |>
    addCohortSubsetDefinition(cs6, targetCohortIds = c(1))

  # Generate cohorts ------------
  cohortTableNames <- getCohortTableNames()

  createCohortTables(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTableNames = cohortTableNames
  )

  generateCohortSet(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTableNames = getCohortTableNames(),
    cohortDefinitionSet = cohortDefinitionSet
  )


  cohorts <- DatabaseConnector::querySql(
    connection = connection,
    sql = "SELECT * FROM main.cohort ORDER BY COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE;"
  )

  # Check the cohort counts to verify the logic worked as expected ---------
  # cohorts # <------ USE TO SEE THE COHORTS TO VERIFY THE INFO BELOW

  # Limit subsets cohort definition 1100 range ------
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1101, ]$COHORT_START_DATE[[1]], lubridate::date("2001-01-01")) # 1101 - First Ever
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1102, ]$COHORT_START_DATE[[1]], lubridate::date("2003-01-01")) # 1102 - Earliest Remaining
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1103, ]$COHORT_START_DATE[[1]], lubridate::date("2005-01-01")) # 1103 - Latest Remaining
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1104, ]$COHORT_START_DATE[[1]], lubridate::date("2007-01-01")) # 1104 - Last Ever
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1105, ]$COHORT_START_DATE[[1]], lubridate::date("2003-01-01")) # 1105 - Calendar #1
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1105, ]$COHORT_START_DATE[[2]], lubridate::date("2005-01-01")) # 1105 - Calendar #2
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1106, ]), 0) # 1106 - First ever + calendar time that restricts to no one
  expect_equal(cohorts[cohorts$COHORT_DEFINITION_ID == 1107, ]$COHORT_START_DATE[[1]], lubridate::date("2003-01-01")) # 1107 - Earliest remaining+calendar restriction

  # Demographic subsets cohort definition 1200 range ------
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1201, ]), 2) # 1201 - Age 2-5
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1202, ]), 4) # 1202 - Gender
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1203, ]), 4) # 1203 - Race
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1204, ]), 4) # 1204 - Ethnicity

  # Cohort subsets cohort definition 1300 range ------
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1301, ]), 2) # 1301 - Subset overlaps cohort start
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1302, ]), 2) # 1302 - Subset overlaps entire target cohort period
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1303, ]), 2) # 1303 - Subset subsumed by entire target cohort period
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1304, ]), 2) # 1304 - Subset overlaps cohort end
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1305, ]), 2) # 1305 - Subset does NOT overlap cohort end - negate
  expect_equal(nrow(cohorts[cohorts$COHORT_DEFINITION_ID == 1306, ]), 2) # 1306 - Subset overlaps target start - tests combo == all
})
