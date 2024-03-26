test_that("Test RxNorm Template", {
  tplDef <- createRxNormCohortTemplateDefinition(cdmDatabaseSchema = "main",
                                                 cohortDatabaseSchema = "main")

  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")

  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  cds <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  expect_true(isCohortDefinitionSet(cds))


  createCohortTables(connection = connection, cohortDatabaseSchema = "main")
  res <- generateCohortSet(connection = connection,
                           cohortDefinitionSet = cds,
                           cdmDatabaseSchema = "main",
                           cohortDatabaseSchema = "main")

  expect_true(all(res$generationStatus == "COMPLETE"))
  expect_true(all(res$cohortId %in% tplDef$cohortId))
})