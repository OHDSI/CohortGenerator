#' OMOP CDM Drug Exposure Sample Data
#'
#' A data set containing sample drug exposures for 2 drugs
#'
#' @format A data frame with 8 rows and 5 variables:
#' \describe{
#'   \item{drug_exposure_id}{A unique identifier for the drug exposure}
#'   \item{person_id}{An integer representing the patient}
#'   \item{drug_concept_id}{An integer concept ID representing the drug concept}
#'   \item{drug_exposure_start_date}{Drug start date}
#'   \item{drug_exposure_end_date}{Drug end date}
#' }
#' @source Fictional data for demonstration.
"omopCdmDrugExposure"

#' OMOP CDM Person Sample Data
#'
#' A data set containing sample persons
#'
#' @format A data frame with 12 rows and 5 variables:
#' \describe{
#'   \item{person_id}{A unique identifier for the person}
#'   \item{gender_concept_id}{An integer concept ID representing the person's gender}
#'   \item{year_of_birth}{Year of birth}
#'   \item{race_concept_id}{An integer concept ID representing the person's race}
#'   \item{ethnicity_concept_id}{An integer concept ID representing the person's ethnicity}
#' }
#' @source Fictional data for demonstration.
"omopCdmPerson"
