dbmsPlatforms <- c("sqlite", "redshift", "postgresql", "oracle", "sql server")
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
outputFolder <- tempdir()