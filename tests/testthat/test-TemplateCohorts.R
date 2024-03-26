test_that("Test RxNorm Template", {
  tplDef <- createRxNormCohortTemplateDefinition(cdmDatabaseSchema = "main",
                                                 cohortDatabaseSchema = "main")

  checkmate::expect_r6(tplDef, "CohortTemplateDefinition")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  tplRefs <- addCohortTemplateDefintion(cohortTemplateDefintion = tplDef, connection = connection)
  expect_true(isCohortDefinitionSet(tplRefs))

  browser()
})