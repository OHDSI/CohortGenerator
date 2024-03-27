test_that("Test RxNorm Template", {
  tplDef <- createRxNormCohortTemplateDefinition(cdmDatabaseSchema = "main",
                                                 cohortDatabaseSchema = "main")

  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  incrementalFolder <- tempfile()
  dir.create(incrementalFolder)
  on.exit({
    DatabaseConnector::disconnect(connection)
    unlink(incrementalFolder, recursive = T, force = T)
  })
  expect_error(addCohortTemplateDefintion(cohortTemplateDefintion = tplDef))
  cds <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  expect_true(isCohortDefinitionSet(cds))
  expect_true(all(cds$isTemplatedCohort))

  createCohortTables(connection = connection, cohortDatabaseSchema = "main")
  res <- generateCohortSet(connection = connection,
                           cohortDefinitionSet = cds,
                           cdmDatabaseSchema = "main",
                           cohortDatabaseSchema = "main",
                           incremental = T,
                           incrementalFolder = incrementalFolder)

  expect_true(all(res$generationStatus == "COMPLETE"))
  expect_true(all(res$cohortId %in% tplDef$cohortId))

  cds2 <- getCohortDefinitionSet(
    settingsFileName = "testdata/name/Cohorts.csv",
    jsonFolder = "testdata/name/cohorts",
    sqlFolder = "testdata/name/sql/sql_server",
    cohortFileNameFormat = "%s",
    cohortFileNameValue = c("cohortName"),
    packageName = "CohortGenerator",
    verbose = FALSE
  )
  cds2 <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  subsetOperations <- list(
    createDemographicSubset(
      name = "Demographic Criteria",
      ageMin = 18,
      ageMax = 64
    )
  )

  subsetDef <- createCohortSubsetDefinition(
    name = "test definition with templates",
    definitionId = 1,
    subsetOperators = subsetOperations
  )

  cds2 <- addCohortSubsetDefinition(cds2, subsetDef)
  # Tests incremental and usage of non-template cohorts in the cohort set as well as subsets of templated cohorts
  res2 <- generateCohortSet(connection = connection,
                            cohortDefinitionSet = cds2,
                            cdmDatabaseSchema = "main",
                            cohortDatabaseSchema = "main",
                            incremental = T,
                            incrementalFolder = incrementalFolder)

  expect_true(all(res2$generationStatus == "SKIPPED"))
  expect_true(all(res2$cohortId %in% tplDef$cohortId))

  # Switch off incremental and regen
  res3 <- generateCohortSet(connection = connection,
                            cohortDefinitionSet = cds,
                            cdmDatabaseSchema = "main",
                            cohortDatabaseSchema = "main",
                            incremental = F)

  expect_true(all(res3$generationStatus == "COMPLETE"))
  # Warning and error if you try and save a template cohort def
  expect_error(expect_warning(saveCohortDefinitionSet(cds)))
})

test_that("Test ATC Template", {
  tplDef <- createAtcCohortTemplateDefinition(cdmDatabaseSchema = "main",
                                              cohortDatabaseSchema = "main")

  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  createCohortTables(connection = connection, cohortDatabaseSchema = "main")
  # Just test sql is valid
  refs <- tplDef$getTemplateReferences(connection = connection)

  tplDef$executeTemplateSql(connection = connection,
                            cohortDatabaseSchema = "main",
                            cdmDatabaseSchema = "main",
                            tempEmulationSchema = NULL,
                            cohortTableNames = getCohortTableNames(),
                            incremental = F,
                            incrementalFolder = NULL)

  #NOTE - No ATC classes included in Eunomia
  expect_error(
    cds <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  )

})