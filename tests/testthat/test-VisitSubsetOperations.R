test_that("Visit subset generation", {
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
    createVisitSubset(
      name = "Visit Subset",
      visitConceptIds = c(1,2,3)
    )
  )

  subsetDef <- createCohortSubsetDefinition(
    name = "test definition",
    definitionId = 1,
    subsetOperators = subsetOperations
  )
  
  cohortDefinitionSetWithSubset <- cohortDefinitionSet %>%
    addCohortSubsetDefinition(subsetDef)
  

  expect_true(nrow(cohortDefinitionSetWithSubset) == 6)
  
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
    incremental = FALSE,
    incrementalFolder = recordKeepingFolder
  )

  unlink(recordKeepingFolder, recursive = TRUE)
})
