.loadJson <- function(definition, simplifyVector = FALSE, simplifyDataFrame = FALSE, ...) {
  if (is.character(definition)) {
    definition <- jsonlite::fromJSON(definition,
                                     simplifyVector = simplifyVector,
                                     simplifyDataFrame = simplifyDataFrame,
                                     ...
    )
  }
  
  if (!is.list(definition)) {
    stop("Cannot instanitate object invalid type ", class(definition))
  }
  definition
}

.toJSON <- function(obj) {
  jsonlite::toJSON(obj, pretty = TRUE)
}

# For R6 classes that have a toList() function, this function handles unboxing of logicals and NAs, lists of
# R6 classes, or just a single R6 class.
.r6ToListOrNA <- function(x) {
  if (length(x) == 0) {
    return(invisible(list()))
  } else if (is.logical(x) && is.na(x)) {
    return(invisible(jsonlite::unbox(NA)))
  } else if (checkmate::testList(x)) {
    return(invisible(lapply(x, function(item) { item$toList() })))
  } else {
    return(invisible(x$toList()))
  }
}

# Applies unbox to scalars and null values, and unlists lists > 0.
.toJsonArray <- function(x) {
  if (checkmate::testScalarNA(x) || checkmate::testNull(x)) {
    return(jsonlite::unbox(NA))
  } else if (length(x) > 0) {
    return(unlist(x))
  }else {
    return(list())
  }
}

# removes any na elements from list
.removeEmpty <- function(x) {
  Filter(Negate(anyNA),x)
}

# Converts list or json into well-formed, empty-removed list.
.convertJSON <- function(data) {
  if (checkmate::testString(data)) {
    return(.removeEmpty(.nullToNa(jsonlite::fromJSON(data, simplifyDataFrame = FALSE))))
  } else if (checkmate::testList(data)) {
    return(.removeEmpty(data))
  } else {
    stop("Error: Attempting to initalize R6 class witn non-list or non-string")
  }
}

# Converts null values to NA to serlize json properly
.nullToNa <- function(obj) {
  if (is.list(obj)) {
    obj <- lapply(obj, function(x) if (is.null(x)) NA else x)
    obj <- lapply(obj, .nullToNa)
  }
  return(obj)
}

