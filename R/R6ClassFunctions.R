
.getAssertChoices <- function(category) {
  readr::read_csv(file = system.file(package = pkgload::pkg_name(), "csv", "assertChoices.csv"), show_col_types = FALSE) |>
    dplyr::filter(category == !!category) |>
    dplyr::pull(choice)
}

.getCaseSql <- function(covariateValues,
                        then) {
  
  sql <- glue::glue("when covariate_value >= {thresholdMin} and covariate_value <= {thresholdMax} then {covariateId}")
  caseSql <- glue::glue("case {sql} end as covariate_id", sql = paste(caseSql, collapse = "\n"))
}

.setString <- function(private, key, value, naOk = FALSE) {
  checkmate::assert_string(x = value, na.ok = naOk, min.chars = 1, null.ok = FALSE)
  private[[key]] <- value
  invisible(private)
}

.setCharacter <- function(private, key, value) {
  checkmate::assert_character(x = value, min.chars = 1, null.ok = FALSE)
  private[[key]] <- value
  invisible(private)
}

.setNumber <- function(private, key, value, nullable = FALSE) {
  checkmate::assert_numeric(x = value, null.ok = nullable)
  private[[key]] <- value
  invisible(private)
}

.setInteger <- function(private, key, value, nullable = FALSE) {
  checkmate::assert_integer(x = value, null.ok = nullable)
  private[[key]] <- value
  invisible(private)
}


.setLogical <- function(private, key, value) {
  checkmate::assert_logical(x = value, null.ok = FALSE)
  private[[key]] <- value
  invisible(private)
}

.setClass <- function(private, key, value, class, nullable = FALSE) {
  checkmate::assert_class(x = value, classes = class, null.ok = nullable)
  private[[key]] <- value
  invisible(private)
}

.setListofClasses <- function(private, key, value, classes) {
  checkmate::assert_list(x = value, types = classes, null.ok = FALSE, min.len = 1)
  private[[key]] <- value
  invisible(private)
}

.setChoice <- function(private, key, value, choices) {
  checkmate::assert_choice(x = value, choices = choices, null.ok = FALSE)
  private[[key]] <- value
  invisible(private)
}

.setChoiceList <- function(private, key, value, choices) {
  checkmate::assert_subset(x = value, choices = choices, empty.ok = FALSE)
  private[[key]] <- value
  invisible(private)
}


.setActiveLogical <- function(private, key, value) {
  # return the value if nothing added
  if(missing(value)) {
    vv <- private[[key]]
    return(vv)
  }
  # replace the codesetTempTable
  .setLogical(private = private, key = key, value = value)
}

.setActiveString <- function(private, key, value) {
  # return the value if nothing added
  if(missing(value)) {
    vv <- private[[key]]
    return(vv)
  }
  # replace the codesetTempTable
  .setString(private = private, key = key, value = value)
}


.setActiveInteger <- function(private, key, value) {
  # return the value if nothing added
  if(missing(value)) {
    vv <- private[[key]]
    return(vv)
  }
  .setInteger(private = private, key = key, value = value)
}
