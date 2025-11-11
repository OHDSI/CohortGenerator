library(CohortGenerator)
resultsFolder <- "E:/TEMP/cg"
databaseId <- "Eunomia"
tablePrefix = "cg_"

if (dir.exists(resultsFolder)) {
  unlink(x = resultsFolder, recursive = TRUE, force = TRUE)
}
dir.create(path = resultsFolder, recursive = TRUE)

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
  subsetCohortNameTemplate = "FOOO @baseCohortName @subsetDefinitionName"
)

cohortDefinitionSet <- cohortDefinitionSet |>
  CohortGenerator::addCohortSubsetDefinition(subsetDef)

# Negative controls
negativeControlOutcomes <- readCsv(file = system.file("testdata/negativecontrols/negativeControlOutcomes.csv",
                                                      package = "CohortGenerator",
                                                      mustWork = TRUE
))

# Run cohort generation process against Eunomia
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "my_cohort_table")
CohortGenerator::runCohortGeneration(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  negativeControlOutcomeCohortSet = negativeControlOutcomes,
  outputFolder = resultsFolder
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
