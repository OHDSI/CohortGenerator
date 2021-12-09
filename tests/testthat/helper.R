connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempdir()

#' Create the Circe cohort expression from a JSON file for generating
#' SQL dynamically
#'
#' @description
#' This function constructs a Circe cohort expression from a JSON file for use
#' with other CirceR functions.
#'
#' @param filePath      The file path containing the Circe JSON file
#'
createCirceExpressionFromFile <- function(filePath) {
  cohortExpression <- readChar(filePath, file.info(filePath)$size)
  return(CirceR::cohortExpressionFromJson(cohortExpression))
}


generateSql <- function(cohortJsonFileName, generateStats = FALSE) {
  cohortExpression <- createCirceExpressionFromFile(cohortJsonFileName)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = generateStats))
  return(cohortSql)
}

# Used to add a SQL column to the "cohorts" data frame
# and toggle if inclusion stats are generated for the given SQL
# definition
getCohortsForTest <- function(cohorts, generateStats = FALSE) {
  cohortSql <- data.frame()
  for (i in 1:nrow(cohorts)) {
    cohortSql <- rbind(cohortSql, data.frame(sql = generateSql(cohorts$cohortJsonFile[i], generateStats)))
  }
  if (length(intersect(colnames(cohorts), c("sql"))) == 1) {
    cohorts$sql <- NULL
  }
  cohorts <- cbind(cohorts, cohortSql)
  return(cohorts)
}

# This will gather all of the cohort JSON in the package for use in the tests
cohortJsonFiles <- list.files(path = system.file("cohorts", package = "CohortGenerator"), full.names = TRUE)
cohorts <- setNames(data.frame(matrix(ncol = 5, nrow = 0), stringsAsFactors = FALSE), c("atlasId", "cohortId","cohortName", "json", "cohortJsonFile"))
for (i in 1:length(cohortJsonFiles)) {
  cohortJsonFileName <- cohortJsonFiles[i]
  cohortFullName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohorts <- rbind(cohorts, data.frame(atlasId = i,
                                       cohortId = i, 
                                       cohortName = cohortFullName, 
                                       json = cohortJson,
                                       cohortJsonFile = cohortJsonFileName,
                                       stringsAsFactors = FALSE))
}
