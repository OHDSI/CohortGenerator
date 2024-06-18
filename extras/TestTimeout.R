# MODIFIED CODE FROM THE R.utils::withTimeout help page ----------------
# - - - - - - - - - - - - - - - - - - - - - - - - -
# Function that takes "a long" time to run
# - - - - - - - - - - - - - - - - - - - - - - - - -
foo <- function() {
  print("Tic")
  for (kk in 1:100) {
    print(kk)
    Sys.sleep(0.1)
    if (kk == 2) {
      stop("tossing error")
    }
  }
  print("Tac")
}

# - - - - - - - - - - - - - - - - - - - - - - - - -
# Evaluate code, if it takes too long, generate
# a timeout by throwing a TimeoutException.
# - - - - - - - - - - - - - - - - - - - - - - - - -
res <- NULL
res <- tryCatch({
  res <- R.utils::withTimeout({
    foo()
  }, timeout = -1)
}, TimeoutException = function(ex) {
  message("Timeout. Skipping.")
  return(1)
}, error = function(ex) {
  message("Actual error")
  return(2)
})

print(res)

# Adaptation for CG ----------------

generationInfo <- tryCatch(expr = {
  startTime <- lubridate::now()
  generationInfo <-  R.utils::withTimeout({
      generationInfo <- runCohortSql(
      startTime = startTime,
      sleep = 10,
      throwError = T
    )}, timeout = 5)
  }, TimeoutException = function(ex) {
    endTime <- lubridate::now()
    warning("Timeout. Skipping cohort generation.")
    return(list(
      generationStatus = "TIMEOUT",
      startTime = startTime,
      endTime = endTime
    ))
  }, error = function(ex) {
    endTime <- lubridate::now()
    ParallelLogger::logError("An error occurred. Error: ", ex)
    # if (stopIfError) {
    #   stop()
    # }
    return(list(
      generationStatus = "FAILED",
      startTime = startTime,
      endTime = endTime
    ))
})

print(generationInfo)
