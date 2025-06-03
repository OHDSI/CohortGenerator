library(jsonlite)
#CHECKING SQL STATEMENT
#to start off, define the target cohort:
celcoxibConceptSet <- Capr::cs(Capr::descendants(1118084), name = "Rx Norm celcoxib")
# 1. Create a cohort for Celcoxib exposure
celcoxibCohort <- Capr::cohort(
  entry = Capr::entry(
    Capr::drugExposure(celcoxibConceptSet),
    observationWindow = Capr::continuousObservation(priorDays = 365)
  ),
  exit = Capr::exit(
    endStrategy = Capr::observationExit()
  )
)


#this is the operator cohort of GI bleeding events
giConceptSet <- Capr::cs(Capr::descendants(192671), name = "Gastrointestinal hemorrhage")
# any GI events
giBleedEvents <- Capr::cohort(
  entry = Capr::entry(
    Capr::conditionOccurrence(giConceptSet)
  )
)

#additional cohort
ibuprofenConceptSet <- Capr::cs(Capr::descendants(1177480), name = "Rx Norm ibuprofen")
# 1. Create a cohort for Celcoxib exposure
ibuprofenCohort <- Capr::cohort(
  entry = Capr::entry(
    Capr::drugExposure(ibuprofenConceptSet),
    observationWindow = Capr::continuousObservation(priorDays = 365)
  ),
  exit = Capr::exit(
    endStrategy = Capr::observationExit()
  )
)


celcoxibSql <- CirceR::buildCohortQuery(
  expression = CirceR::cohortExpressionFromJson(Capr::as.json(celcoxibCohort)),
  options = CirceR::createGenerateOptions(generateStats = TRUE)
)

giSql <- CirceR::buildCohortQuery(
  expression = CirceR::cohortExpressionFromJson(Capr::as.json(giBleedEvents)),
  options = CirceR::createGenerateOptions(generateStats = TRUE)
)

ibuprofenSql <- CirceR::buildCohortQuery(
  expression = CirceR::cohortExpressionFromJson(Capr::as.json(ibuprofenCohort)),
  options = CirceR::createGenerateOptions(generateStats = TRUE)
)

#has the target, operator, and ibuprofen exposure
cohortDefinitionSet <- tibble::tibble(
  cohortId = c(1,2,3),
  cohortName = c("celcoxib", "GI Bleed", "ibuprofen"),
  sql = c(celcoxibSql, giSql,ibuprofenSql),
  json = c(Capr::as.json(celcoxibCohort), Capr::as.json(giBleedEvents), Capr::as.json(ibuprofenCohort))
)

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

# #Save to JSON
cohortList <- lapply(1:length(cohortDefinitionSet$cohortId), function(i) {
  list(
    cohortId = cohortDefinitionSet$cohortId[i],
    cohortName = cohortDefinitionSet$cohortName[i],
    sql = cohortDefinitionSet$sql[i],
    json = cohortDefinitionSet$json[i],
    subsetParent = cohortDefinitionSet$subsetParent[i],
    isSubset = cohortDefinitionSet$isSubset[i],
    subsetDefinitionId = cohortDefinitionSet$subsetDefinitionId[i]
  )
})

# Convert the cohort list into a JSON formatted string with pretty printing
jsonFormatted <- toJSON(cohortList, pretty = TRUE, auto_unbox = TRUE)

# Save the JSON to a file
jsonFilePath <- "inst/testdata/SaveCohorts.JSON"
write(jsonFormatted, file = jsonFilePath)


