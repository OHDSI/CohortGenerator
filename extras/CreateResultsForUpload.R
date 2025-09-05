library(CohortGenerator)
resultsFolder <- "D:/TEMP/cg"
databaseId <- "Eunomia"
tablePrefix = "cg_"

if (!dir.exists(resultsFolder)) {
  dir.create(path = resultsFolder, recursive = TRUE)
}

# First construct a cohort definition set: an empty 
# data frame with the cohorts to generate
cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this 
# package as an example
cohortJsonFiles <- list.files(path = system.file("testdata/name/cohorts", package = "CohortGenerator"), full.names = TRUE)
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  # Here we read in the JSON in order to create the SQL
  # using [CirceR](https://ohdsi.github.io/CirceR/)
  # If you have your JSON and SQL stored differenly, you can
  # modify this to read your JSON/SQL files however you require
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = TRUE))
  cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(cohortId = as.numeric(i),
                                                               cohortName = cohortName, 
                                                               json = cohortJson,
                                                               sql = cohortSql,
                                                               stringsAsFactors = FALSE))
}

subsetOperations <- list(
  createDemographicSubsetOperator(
    name = "Demographic Criteria 1",
    ageMin = 18,
    ageMax = 64
  ),
  createDemographicSubsetOperator(
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

cohortDefinitionSet <- cohortDefinitionSet |>
  CohortGenerator::addCohortSubsetDefinition(subsetDef)

# Massage and save the cohort definition set
colsToRename <- c("cohortId", "cohortName", "sql", "json")
colInd <- which(names(cohortDefinitionSet) %in% colsToRename)
cohortDefinitions <- cohortDefinitionSet
names(cohortDefinitions)[colInd] <- c("cohortDefinitionId", "cohortName", "sqlCommand", "json")
cohortDefinitions$description <- ""
CohortGenerator::writeCsv(
  x = cohortDefinitions,
  file = file.path(resultsFolder, "cohort_definition.csv")
)

# Export the subsets
subsets <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
if (length(subsets)) {
  dfs <- lapply(subsets, function(x) {
    data.frame(subsetDefinitionId = x$definitionId, json = as.character(x$toJSON()))
  })
  subsetDefinitions <- data.frame()
  for (subsetDef in dfs) {
    subsetDefinitions <- rbind(subsetDefinitions, subsetDef)
  }
  
  CohortGenerator::writeCsv(
    x = subsetDefinitions,
    file = file.path(resultsFolder, "cohort_subset_definition.csv")
  )
}


# Generate the cohort set against Eunomia. 
# cohortsGenerated contains a list of the cohortIds 
# successfully generated against the CDM
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "my_cohort_table")
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortDatabaseSchema = "main",
                                    cohortTableNames = cohortTableNames)
# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = "main",
                                                       cohortDatabaseSchema = "main",
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = cohortDefinitionSet)


cohortsGenerated$databaseId <- databaseId
CohortGenerator::writeCsv(
  x = cohortsGenerated,
  file = file.path(resultsFolder, "cohort_generation.csv")
)

# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                 cohortDatabaseSchema = "main",
                                                 cohortTable = cohortTableNames$cohortTable,
                                                 databaseId = databaseId)
cohortCounts <- cohortCounts[c("databaseId", "cohortId", "cohortEntries", "cohortSubjects")]

CohortGenerator::writeCsv(
  x = cohortCounts,
  file = file.path(resultsFolder, "cohort_count.csv")
)

# Generate the negative controls
negativeControlOutcomes <- readCsv(file = system.file("testdata/negativecontrols/negativeControlOutcomes.csv",
                                                      package = "CohortGenerator",
                                                      mustWork = TRUE
))
negativeControlOutcomes$cohortId <- negativeControlOutcomes$outcomeConceptId


CohortGenerator::generateNegativeControlOutcomeCohorts(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = cohortTableNames$cohortTable,
  negativeControlOutcomeCohortSet = negativeControlOutcomes,
  occurrenceType = "all",
  detectOnDescendants = TRUE,
  incremental = F
)

cohortCountsNegativeControlOutcomes <- CohortGenerator::getCohortCounts(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = "main",
  cohortTable = cohortTableNames$cohortTable,
  databaseId = databaseId,
  cohortIds = negativeControlOutcomes$cohortId
)

CohortGenerator::writeCsv(
  x = cohortCountsNegativeControlOutcomes,
  file = file.path(resultsFolder, "cohort_count_neg_ctrl.csv")
)

CohortGenerator::exportCohortStatsTables(
  connectionDetails = connectionDetails,
  cohortStatisticsFolder = resultsFolder,
  cohortDatabaseSchema = "main",
  databaseId = databaseId,
  snakeCaseToCamelCase = FALSE,
  fileNamesInSnakeCase = TRUE,  
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet
)

# Set the table names in resultsDataModelSpecification.csv
resultsDataModel <- CohortGenerator::getResultsDataModelSpecifications()
oldTableNames <- gsub("cg_", "", resultsDataModel$tableName)
file.rename(
  file.path(resultsFolder, paste0(unique(oldTableNames), ".csv")),
  file.path(resultsFolder, paste0(unique(resultsDataModel$tableName), ".csv"))
)
CohortGenerator::writeCsv(
  x = resultsDataModel,
  file = file.path(resultsFolder, "resultsDataModelSpecification.csv"),
  warnOnCaseMismatch = FALSE,
  warnOnFileNameCaseMismatch = FALSE,
  warnOnUploadRuleViolations = FALSE
)


# Test the upload
CohortGenerator::createResultsDataModel(
  connectionDetails = connectionDetails,
  databaseSchema = "main",
)

CohortGenerator::uploadResults(
  connectionDetails = connectionDetails,
  schema = "main",
  resultsFolder = resultsFolder,
  purgeSiteDataBeforeUploading = F
)

#conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)

zip::zip(
  zipfile = file.path("inst", "testdata", "Results_Eunomia.zip"),
  files = list.files(resultsFolder, full.names = T),
  mode = "cherry-pick"
)

unlink(connectionDetails$server())
unlink(resultsFolder, recursive = TRUE)
