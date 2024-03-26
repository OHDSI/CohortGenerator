test_that("Test RxNorm Template", {
  tplDef <- createRxNormCohortTemplateDefinition(cdmDatabaseSchema = "main",
                                                 cohortDatabaseSchema = "main")

  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  cds <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  expect_true(isCohortDefinitionSet(cds))
  expect_true(all(cds$isTemplatedCohort))

  createCohortTables(connection = connection, cohortDatabaseSchema = "main")
  res <- generateCohortSet(connection = connection,
                           cohortDefinitionSet = cds,
                           cdmDatabaseSchema = "main",
                           cohortDatabaseSchema = "main")

  expect_true(all(res$generationStatus == "COMPLETE"))
  expect_true(all(res$cohortId %in% tplDef$cohortId))
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