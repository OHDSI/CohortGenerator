
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
  purrr::map(cohortDefinition$ConceptSets, function(csExpression) {

    conceptSet <- data.frame()
    for (item in csExpression$expression$items) {
      conceptSet <- conceptSet |>
        dplyr::bind_rows(
          data.frame(conceptId = item$concept$CONCEPT_ID,
                     isExcluded = as.integer(item$isExcluded),
                     includeDescendants = as.integer(item$includeDescendants),
                     includeMapped = as.integer(item$includeMapped)))
    }

    conceptSets[[csExpression$name]] <<- conceptSet
  })

  return(conceptSets)
}
