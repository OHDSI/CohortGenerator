
getConceptSetChecksum <- function(conceptSet) {
  checksum <- conceptSet |>
    dplyr::select("conceptId", "includeDescendants", "includeMapped", "isExcluded") |>
    dplyr::mutate(isExcluded = as.integer(.data$isExcluded),
                  includeDescendants = as.integer(.data$includeDescendants),
                  includeMapped = as.integer(.data$includeMapped)) |>
    dplyr::arrange(.data$conceptId, .data$isExcluded, .data$includeDescendants, .data$includeMapped) |>
    digest::digest(algo = "sha256")

  return(checksum)
}


extractCirceConceptSets <- function(cohortDefinition) {
  conceptSets <- list()

  # Loop through each concept set expression in the definition
  for (csIndex in seq_along(cohortDefinition$ConceptSets)) {
    csExpression <- cohortDefinition$ConceptSets[[csIndex]]

    conceptSet <- data.frame()

    # Loop through each item in the concept set expression
    for (itemIndex in seq_along(csExpression$expression$items)) {
      item <- csExpression$expression$items[[itemIndex]]
      newRow <- data.frame(
        conceptId = item$concept$CONCEPT_ID,
        isExcluded = ifelse(length(item$isExcluded), as.integer(item$isExcluded), 0),
        includeDescendants =  ifelse(length(item$includeDescendants), as.integer(item$includeDescendants), 0),
        includeMapped =  ifelse(length(item$includeMapped), as.integer(item$includeMapped), 0)
      )

      # Bind the new row to the existing conceptSet
      conceptSet <- dplyr::bind_rows(conceptSet, newRow)
    }

    # Save the complete conceptSet data frame in the list
    conceptSets[[csExpression$name]] <- conceptSet
  }

  return(conceptSets)
}