library(testthat)
library(CohortGenerator)

test_that("Record keeping of single type tasks", {
  rkf <- tempfile()
  
  sql1 <- "SELECT * FROM my_table WHERE x = 1;"
  checksum1 <- computeChecksum(sql1)
  expect_true(
    isTaskRequired(
      cohortId = 1,
      runSql = TRUE,
      checksum = checksum1,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 1,
    runSql = TRUE,
    checksum = checksum1,
    recordKeepingFile = rkf
  )
  
  
  expect_false(
    isTaskRequired(
      cohortId = 1,
      runSql = TRUE,
      checksum = checksum1,
      recordKeepingFile = rkf
    )
  )
  
  sql2 <- "SELECT * FROM my_table WHERE x = 2;"
  checksum2 <- computeChecksum(sql2)
  expect_true(
    isTaskRequired(
      cohortId = 2,
      runSql = TRUE,
      checksum = checksum2,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 2,
    runSql = TRUE,
    checksum = checksum2,
    recordKeepingFile = rkf
  )
  
  sql1a <- "SELECT * FROM my_table WHERE x = 1 AND y = 2;"
  checksum1a <- computeChecksum(sql1a)
  expect_true(
    isTaskRequired(
      cohortId = 1,
      runSql = TRUE,
      checksum = checksum1a,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 1,
    runSql = TRUE,
    checksum = checksum1a,
    recordKeepingFile = rkf
  )
  
  expect_false(
    isTaskRequired(
      cohortId = 1,
      runSql = TRUE,
      checksum = checksum1a,
      recordKeepingFile = rkf
    )
  )
  
  unlink(rkf)
})

test_that("Record keeping of multiple type tasks", {
  rkf <- tempfile()
  
  sql1 <- "SELECT * FROM my_table WHERE x = 1;"
  checksum1 <- computeChecksum(sql1)
  expect_true(
    isTaskRequired(
      cohortId = 1,
      task = "Run SQL",
      checksum = checksum1,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 1,
    task = "Run SQL",
    checksum = checksum1,
    recordKeepingFile = rkf
  )
  
  
  expect_false(
    isTaskRequired(
      cohortId = 1,
      task = "Run SQL",
      checksum = checksum1,
      recordKeepingFile = rkf
    )
  )
  
  sql2 <- "SELECT * FROM my_table WHERE x = 1 AND y = 1;"
  checksum2 <- computeChecksum(sql2)
  expect_true(
    isTaskRequired(
      cohortId = 1,
      cohortId2 = 2,
      task = "Compare cohorts",
      checksum = checksum2,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 1,
    cohortId2 = 2,
    task = "Compare cohorts",
    checksum = checksum2,
    recordKeepingFile = rkf
  )
  
  expect_false(
    isTaskRequired(
      cohortId = 1,
      task = "Run SQL",
      checksum = checksum1,
      recordKeepingFile = rkf
    )
  )
  
  
  sql2a <- "SELECT * FROM my_table WHERE x = 1 AND y = 2 AND z = 3;"
  checksum2a <- computeChecksum(sql2a)
  expect_true(
    isTaskRequired(
      cohortId = 1,
      cohortId2 = 2,
      task = "Compare cohorts",
      checksum = checksum2a,
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = 1,
    cohortId2 = 2,
    task = "Compare cohorts",
    checksum = checksum2a,
    recordKeepingFile = rkf
  )
  
  expect_false(
    isTaskRequired(
      cohortId = 1,
      cohortId2 = 2,
      task = "Compare cohorts",
      checksum = checksum2a,
      recordKeepingFile = rkf
    )
  )
  
  unlink(rkf)
})

test_that("Record keeping of multiple tasks at once", {
  rkf <- tempfile()
  
  task <- dplyr::tibble(
    cohortId = c(1, 2),
    sql = c(
      "SELECT * FROM my_table WHERE x = 1;",
      "SELECT * FROM my_table WHERE x = 2;"
    )
  )
  task$checksum <- computeChecksum(task$sql)
  expect_true(
    isTaskRequired(
      cohortId = task$cohortId[1],
      checksum = task$checksum[1],
      recordKeepingFile = rkf
    )
  )
  
  recordTasksDone(
    cohortId = task$cohortId,
    checksum = task$checksum,
    recordKeepingFile = rkf
  )
  
  
  expect_false(
    isTaskRequired(
      cohortId = task$cohortId[1],
      checksum = task$checksum[1],
      recordKeepingFile = rkf
    )
  )
  
  expect_false(
    isTaskRequired(
      cohortId = task$cohortId[2],
      checksum = task$checksum[2],
      recordKeepingFile = rkf
    )
  )
  
  
  task <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    sql = c(
      "SELECT * FROM my_table WHERE x = 3;",
      "SELECT * FROM my_table WHERE x = 4;",
      "SELECT * FROM my_table WHERE x = 5;"
    )
  )
  task$checksum <- computeChecksum(task$sql)
  
  expect_true(
    isTaskRequired(
      cohortId = task$cohortId[1],
      checksum = task$checksum[1],
      recordKeepingFile = rkf
    )
  )
  
  tasks <- getRequiredTasks(
    cohortId = task$cohortId,
    checksum = task$checksum,
    recordKeepingFile = rkf
  )
  expect_equal(nrow(tasks), 3)
  
  recordTasksDone(
    cohortId = task$cohortId,
    checksum = task$checksum,
    recordKeepingFile = rkf
  )
  
  expect_false(
    isTaskRequired(
      cohortId = task$cohortId[1],
      checksum = task$checksum[1],
      recordKeepingFile = rkf
    )
  )
  
  expect_false(
    isTaskRequired(
      cohortId = task$cohortId[2],
      checksum = task$checksum[2],
      recordKeepingFile = rkf
    )
  )
  
  expect_false(
    isTaskRequired(
      cohortId = task$cohortId[3],
      checksum = task$checksum[3],
      recordKeepingFile = rkf
    )
  )
  
  tasks <- getRequiredTasks(
    cohortId = task$cohortId[2],
    checksum = task$checksum[2],
    recordKeepingFile = rkf
  )
  expect_equal(nrow(tasks), 0)
  
  unlink(rkf)
})


test_that("Incremental save", {
  tmpFile <- tempfile()
  data <- dplyr::tibble(cohortId = c(1, 1, 2, 2, 3),
                        count = c(100, 200, 300, 400, 500))
  saveIncremental(data, tmpFile, cohortId = c(1, 2, 3))
  
  newData <- dplyr::tibble(cohortId = c(1, 2, 2),
                           count = c(600, 700, 800))
  
  saveIncremental(newData, tmpFile, cohortId = c(1, 2))
  
  
  
  goldStandard <- dplyr::tibble(cohortId = c(3, 1, 2, 2),
                                count = c(500, 600, 700, 800))
  
  incrementalFileContents <- readr::read_csv(
    tmpFile,
    col_types = readr::cols()
  )
  
  expect_equal(nrow(goldStandard), nrow(incrementalFileContents))
  for(i in 1:nrow(goldStandard)) {
    for(j in colnames(goldStandard)) {
      expect_equal(goldStandard[[j]][i], incrementalFileContents[[j]][i])
    }
  }
  unlink(tmpFile)
})

test_that("Incremental save with empty key", {
  tmpFile <- tempfile()
  data <- dplyr::tibble(cohortId = c(1, 1, 2, 2, 3),
                        count = c(100, 200, 300, 400, 500))
  saveIncremental(data, tmpFile, cohortId = c(1, 2, 3))
  
  newData <- dplyr::tibble()
  
  saveIncremental(newData, tmpFile, cohortId = c())
  
  incrementalFileContents <- readr::read_csv(
    tmpFile,
    col_types = readr::cols()
  )
  expect_equal(nrow(data), nrow(incrementalFileContents))
  for(i in 1:nrow(data)) {
    for(j in colnames(data)) {
      expect_equal(data[[j]][i], incrementalFileContents[[j]][i])
    }
  }
  unlink(tmpFile)
})
