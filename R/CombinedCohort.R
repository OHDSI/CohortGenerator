# CombinedCohortOp -------------
#' @title A Combined cohort operation used to UNION or INTERSECT cohorts.  Note: only UNION supported.
#' @export
#' @description
#' Defines a UNION or INTERSECT on a set of cohorts.
CombinedCohortOp <- R6::R6Class(
  classname = "CombinedCohortOp",
  private = list(
    .targetCohortIds = NULL,
    .opType = "union"
  ),
  active = list(
    #' @field targetCohortIds The list of cohorts to apply in this group.
    targetCohortIds = function(targetCohortIds) {
      if (missing(targetCohortIds)) {
        return(private$.targetCohortIds)
      }
      checkmate::assertList(as.list(targetCohortIds), types="numeric", min.len = 0)
      private$.targetCohortIds <- targetCohortIds
      return(self)
    },
    #' @field opType The group operation, either 'union' or 'intersect'
    opType = function(opType) {
      if (missing(opType)) {
        return(private$.opType)
      }
      checkmate::assertChoice(opType, c("union"))
      private$.opType <- opType
      return(self)
    }
  ),
  public = list(
    #' @description
    #' creates a new instance, using the provided data param if provided.
    #' @param data the data (as a json string or list) to initialize with
    initialize = function(data = list()) {
      dataList <- .convertJSON(data)
      
      if ("targetCohortIds" %in% names (dataList)) self$targetCohortIds <- dataList$targetCohortIds
      if ("opType" %in% names (dataList)) self$opType <- dataList$opType
    },
    #' @description List representation of object
    toList = function() {
      .removeEmpty(list(
        targetCohortIds = .toJsonArray(private$.targetCohortIds),
        opType = jsonlite::unbox(private$.opType)
      ))      
    },
    #' To JSON
    #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },
    
    #' Is Equal to
    #' @description Compare CombinedCohortGroup to another
    #' @param other CombinedCohortGroup instance
    isEqualTo = function(other) {
      checkmate::assertR6(other, "CombinedCohortGroup")
      return(other$toJSON() == self$toJSON())
    },
    #' getDependentCohortIds
    #' @description Gets the dependent cohortIds from this operation
    getDependentCohortIds = function() {
      if (length(private$.targetCohortIds) == 0) return(c())
      return(private$.targetCohortIds)
    }
  )
)

# CombinedCohortDef -------------
#' @title A CombinedCohortDef
#' @export
#' @description
#' Defines the ID, name and operation to produce a new cohort.
CombinedCohortDef <- R6::R6Class(
  classname = "CombinedCohortDef",
  private = list(
    .cohortId = NA,
    .cohortName = NA,
    .expression = NULL
  ),
  active = list(
    #' @field cohortId The list of cohorts to apply in this group.
    cohortId = function(cohortId) {
      if (missing(cohortId)) {
        return(private$.cohortId)
      }
      checkmate::assertInt(cohortId)
      private$.cohortId <- cohortId
      return(self)
    },
    #' @field cohortName the name given to this outcome definition
    cohortName = function(cohortName) {
      if (missing(cohortName)) {
        private$.cohortName
      } else {
        # check type
        checkmate::assertCharacter(cohortName)
        private$.cohortName <- cohortName
        self
      }
    },    
    #' @field expression The combo operator that is the root of the definition.
    expression = function(expression) {
      if (missing(expression)) {
        return(private$.expression)
      }
      checkmate::assertClass(expression, classes="CombinedCohortOp")
      private$.expression <- expression
      return(self)
    }
  ),
  public = list(
    #' @description
    #' creates a new instance, using the provided data param if provided.
    #' @param data the data (as a json string or list) to initialize with
    initialize = function(data = list()) {
      dataList <- .convertJSON(data)
      
      if ("cohortId" %in% names (dataList)) self$targetCohortIds <- dataList$cohortId
      if ("cohortName" %in% names (dataList)) self$cohortName <- dataList$cohortName
      if ("expression" %in% names (dataList)) self$expression<- CohortGenerator::CombinedCohortOp$new(dataList$expression)
    },
    #' @description List representation of object
    toList = function() {
      .removeEmpty(list(
        cohortId = jsonlite::unbox(private$.cohortId),
        cohortName = jsonlite::unbox(private$.cohortName),
        expression = .r6ToListOrNA(private$.expression)
      ))      
    },
    #' To JSON
    #' @description json serialized representation of object
    toJSON = function() {
      .toJSON(self$toList())
    },
    
    #' Is Equal to
    #' @description Compare CombinedCohortDef to another
    #' @param other CombinedCohortDef instance
    isEqualTo = function(other) {
      checkmate::assertR6(other, "CombinedCohortDef")
      return(other$toJSON() == self$toJSON())
    }
  )
)

### Factory Functions

#' Create CombinedCohortOp instance
#' @description
#' A factory function to create CombinedCohortOp
#' @export
#' @param targetCohortIds           list of target cohort IDs to combine in this operation
#' @param opType                    The op type of this cohort combination, can only be 'union'
createCombinedCohortOp <- function(targetCohortIds, opType) {
  
  cohortOp <- CombinedCohortOp$new()
  if (!missing(targetCohortIds)) cohortOp$targetCohortIds <- targetCohortIds
  if (!missing(opType)) cohortOp$opType <- opType

  return (cohortOp);
}

#' Defines a combined cohort using combined cohort operations
#' @description
#' Creates an instance of CombinedCohortDef with the provided cohortId, cohortName and the combine operator expression
#' @export
#' @param cohortId       The output cohort id from applying the combine expression.
#' @param cohortName     The output cohort name
#' @param expression     The combine operator that will yield the final cohort.
createCombinedCohortDef <- function(cohortId, cohortName, expression) {
  
  cohortDef <- CombinedCohortDef$new();
  if (!missing(cohortId)) cohortDef$cohortId <- cohortId;
  if (!missing(cohortName)) cohortDef$cohortName <- cohortName;
  if (!missing(expression)) cohortDef$expression <- expression;
  
  return (cohortDef);
}



#' Add cohort subset definition to a cohort definition set
#' @description
#' Given a subset definition and cohort definition set, this function returns a modified cohortDefinitionSet
#' That contains cohorts that's have parent's contained within the base cohortDefinitionSet
#'
#' Also adds the columns subsetParent and isSubset that denote if the cohort is a subset and what the parent definition
#' is.
#' @export
#' @param cohortDefinitionSet       data.frame that conforms to CohortDefinitionSet
#' @param combinedCohortDefiniton   CombinedCohortDefinition instance
#' @param overwriteExisting         Overwrite existing subset definition of the same definitionId if present
addCombinedCohort <- function(cohortDefinitionSet,
                                      combinedCohortDefiniton,
                                      overwriteExisting = FALSE) {
  checkmate::assertTRUE(isCohortDefinitionSet(cohortDefinitionSet))
  checkmate::assertR6(combinedCohortDefiniton, "CombinedCohortDef")
  checkmate::assertTRUE(!is.null(combinedCohortDefiniton$expression))
  
  if (!"dependentCohorts" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$dependentCohorts <- ""
  }
  
  if (!"isCombinedCohort" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$isCombinedCohort <- FALSE
  }
  
  dependentCohortIds <- combinedCohortDefiniton$expression$getDependentCohortIds()
  if (length(dependentCohortIds) > 0) {
    checkmate::assertSubset(dependentCohortIds, cohortDefinitionSet$cohortId)
  }

  if (!overwriteExisting && nrow(cohortDefinitionSet %>% dplyr::filter(.data$cohortId == combinedCohortDefiniton$cohortId)) > 0) {
    stop("The specified cohortId for this combined cohort already exists in the cohort definition set")
  } else {
    # remove this definition from the set
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId != combinedCohortDefiniton$cohortId)
  }

  defSql <- SqlRender::readSql(system.file("sql", "sql_server", "combinedCohorts", "CombinedCohortDefinition.sql", package = "CohortGenerator"))
  
  queryBuilder <- CombinedCohortQueryBuilder$new()
  combinationQuery <- queryBuilder$buildQuery(combinedCohortDefiniton)
  
  defSql <- SqlRender::render(defSql, 
                              output_cohort_id = combinedCohortDefiniton$cohortId,
                              combined_cohort_query = combinationQuery)
  
  cohortDefinitionSet <-
    dplyr::bind_rows(
      cohortDefinitionSet,
      data.frame(
        cohortId = combinedCohortDefiniton$cohortId,
        cohortName = combinedCohortDefiniton$cohortName,
        sql = defSql,
        json = as.character(combinedCohortDefiniton$toJSON()),
        isCombinedCohort = TRUE,
        dependentCohorts = paste0(dependentCohortIds, collapse = ",")
      )
    )
  
  attr(cohortDefinitionSet, "hasCombinedCohorts") <- TRUE
  
  return(cohortDefinitionSet)
}

