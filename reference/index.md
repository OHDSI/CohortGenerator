# Package index

## Cohort Generation

Functions that support generating cohorts.

- [`runCohortGeneration()`](https://ohdsi.github.io/CohortGenerator/reference/runCohortGeneration.md)
  : Run a cohort generation and export results
- [`generateCohortSet()`](https://ohdsi.github.io/CohortGenerator/reference/generateCohortSet.md)
  : Generate a set of cohorts

## Cohort Tables

Functions that support creating the necessary cohort tables.

- [`createCohortTables()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortTables.md)
  : Create cohort tables
- [`getCohortTableNames()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  : Used to get a list of cohort table names to use when creating the
  cohort tables

## Cohort Defintion Set

Functions that support working with a cohort definition set

- [`saveCohortDefinitionSet()`](https://ohdsi.github.io/CohortGenerator/reference/saveCohortDefinitionSet.md)
  : Save the cohort definition set to the file system
- [`getCohortDefinitionSet()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortDefinitionSet.md)
  : Get a cohort definition set
- [`createEmptyCohortDefinitionSet()`](https://ohdsi.github.io/CohortGenerator/reference/createEmptyCohortDefinitionSet.md)
  : Create an empty cohort definition set
- [`checkAndFixCohortDefinitionSetDataTypes()`](https://ohdsi.github.io/CohortGenerator/reference/checkAndFixCohortDefinitionSetDataTypes.md)
  : Check if a cohort definition set is using the proper data types
- [`isCohortDefinitionSet()`](https://ohdsi.github.io/CohortGenerator/reference/isCohortDefinitionSet.md)
  : Is the data.frame a cohort definition set?

## Cohort Counts

Function for obtaining the counts of subjects and events for one or more
cohorts

- [`getCohortCounts()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortCounts.md)
  : Count the cohort(s)

## Cohort Subset Functions

Functions for creating cohort subset definitions and subset operators.

- [`addCohortSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addCohortSubsetDefinition.md)
  : Add cohort subset definition to a cohort definition set
- [`createCohortSubset()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortSubset.md)
  : Create Cohort Subset Operator
- [`createCohortSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortSubsetOperator.md)
  : A definition of subset functions to be applied to a set of cohorts
- [`createCohortSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortSubsetDefinition.md)
  : Create Subset Definition
- [`createDemographicSubset()`](https://ohdsi.github.io/CohortGenerator/reference/createDemographicSubset.md)
  : Create Demographic Subset Operator
- [`createDemographicSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createDemographicSubsetOperator.md)
  : Create createDemographicSubset Subset operator
- [`createLimitSubset()`](https://ohdsi.github.io/CohortGenerator/reference/createLimitSubset.md)
  : Create Limit Subset Operator
- [`createLimitSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createLimitSubsetOperator.md)
  : Create Limit Subset Operator
- [`createSubsetCohortWindow()`](https://ohdsi.github.io/CohortGenerator/reference/createSubsetCohortWindow.md)
  : Create a relative time window for cohort subset operations
- [`getSubsetDefinitions()`](https://ohdsi.github.io/CohortGenerator/reference/getSubsetDefinitions.md)
  : Get cohort subset definitions from a cohort definition set
- [`saveCohortSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/saveCohortSubsetDefinition.md)
  : Save cohort subset definitions to json

## Cohort Subset Recipies

Standard functions for creating subsets that wrap more complex
operations

- [`addExcludeOnIndexSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addExcludeOnIndexSubsetDefinition.md)
  : Add exclude on index subset definition
- [`addIndicationSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addIndicationSubsetDefinition.md)
  : Add Indication Subset Definition
- [`addRestrictionSubsetDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addRestrictionSubsetDefinition.md)
  : Add Restriction Subset Definition
- [`getExcludeOnIndexSubsetDefinitionIds()`](https://ohdsi.github.io/CohortGenerator/reference/getExcludeOnIndexSubsetDefinitionIds.md)
  : Get Exclude On Index Subset Definition Ids
- [`getIndicationSubsetDefinitionIds()`](https://ohdsi.github.io/CohortGenerator/reference/getIndicationSubsetDefinitionIds.md)
  : Get Indication Subset Definition Ids
- [`getRestrictionSubsetDefinitionIds()`](https://ohdsi.github.io/CohortGenerator/reference/getRestrictionSubsetDefinitionIds.md)
  : Get Restriction Subset Definition Ids

## Cohort Template Functions

Utilities for creating template cohort definitions from raw sql

- [`CohortTemplateDefinition`](https://ohdsi.github.io/CohortGenerator/reference/CohortTemplateDefinition.md)
  : Class for automating the creation of bulk cohorts
- [`addCohortTemplateDefintion()`](https://ohdsi.github.io/CohortGenerator/reference/addCohortTemplateDefintion.md)
  : Add Cohort template definition to cohort set
- [`addSqlCohortDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addSqlCohortDefinition.md)
  : Add an sql cohort definition
- [`addUnionCohortDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/addUnionCohortDefinition.md)
  : Add union cohort definition to cohort definition set
- [`createAtcCohortTemplateDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/createAtcCohortTemplateDefinition.md)
  : Create ATC Cohort Template Definition
- [`createCohortTemplateDefintion()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortTemplateDefintion.md)
  : Create Cohort Template Definition
- [`createRxNormCohortTemplateDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/createRxNormCohortTemplateDefinition.md)
  : Create Rx Norm Cohort Template Definition
- [`createSnomedCohortTemplateDefinition()`](https://ohdsi.github.io/CohortGenerator/reference/createSnomedCohortTemplateDefinition.md)
  : Create SNOMED Cohort Template Definition
- [`createUnionCohortTemplate()`](https://ohdsi.github.io/CohortGenerator/reference/createUnionCohortTemplate.md)
  : Create cohort template to union multiple cohorts
- [`getCohortValidationCounts()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortValidationCounts.md)
  : Validate cohort
- [`getTemplateDefinitions()`](https://ohdsi.github.io/CohortGenerator/reference/getTemplateDefinitions.md)
  : Extract template definitions from a cohort definition set

## Cohort Subset Classes

R6 classes for cohort subset definitions and subset operators.

- [`CohortSubsetDefinition`](https://ohdsi.github.io/CohortGenerator/reference/CohortSubsetDefinition.md)
  : Cohort Subset Definition
- [`CohortSubsetOperator`](https://ohdsi.github.io/CohortGenerator/reference/CohortSubsetOperator.md)
  : Cohort Subset Operator
- [`DemographicSubsetOperator`](https://ohdsi.github.io/CohortGenerator/reference/DemographicSubsetOperator.md)
  : Demographic Subset Operator
- [`LimitSubsetOperator`](https://ohdsi.github.io/CohortGenerator/reference/LimitSubsetOperator.md)
  : Limit Subset Operator
- [`SubsetCohortWindow`](https://ohdsi.github.io/CohortGenerator/reference/SubsetCohortWindow.md)
  : Time Window For Cohort Subset Operator
- [`SubsetOperator`](https://ohdsi.github.io/CohortGenerator/reference/SubsetOperator.md)
  : Abstract base class for subsets.

## Cohort Statistics

Functions for inserting inclusion rule names from a cohort definition,
exporting the cohort statistics to the file system and a helper function
for dropping those tables when they are no longer needed. These
functions assume you are using
[Circe](https://github.com/OHDSI/circe-be) for inclusion rules and
cohort statistics.

- [`computeCohortAttrition()`](https://ohdsi.github.io/CohortGenerator/reference/computeCohortAttrition.md)
  : Compute cohort attrition from inclusion rule statistics
- [`getCohortStats()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortStats.md)
  : Get Cohort Inclusion Stats Table Data
- [`getCohortInclusionRules()`](https://ohdsi.github.io/CohortGenerator/reference/getCohortInclusionRules.md)
  : Get Cohort Inclusion Rules from a cohort definition set
- [`insertInclusionRuleNames()`](https://ohdsi.github.io/CohortGenerator/reference/insertInclusionRuleNames.md)
  : Used to insert the inclusion rule names from a cohort definition set
  when generating cohorts that include cohort statistics
- [`exportCohortStatsTables()`](https://ohdsi.github.io/CohortGenerator/reference/exportCohortStatsTables.md)
  : Export the cohort statistics tables to the file system
- [`exportCohortSubsetStatsTables()`](https://ohdsi.github.io/CohortGenerator/reference/exportCohortSubsetStatsTables.md)
  : Export cohort subset statistics tables to the file system
- [`dropCohortStatsTables()`](https://ohdsi.github.io/CohortGenerator/reference/dropCohortStatsTables.md)
  : Drop cohort statistics tables

## Negative Control Outcomes

Functions for creating negative control outcome cohorts for use in
population-level estimation.

- [`createEmptyNegativeControlOutcomeCohortSet()`](https://ohdsi.github.io/CohortGenerator/reference/createEmptyNegativeControlOutcomeCohortSet.md)
  : Create an empty negative control outcome cohort set
- [`generateNegativeControlOutcomeCohorts()`](https://ohdsi.github.io/CohortGenerator/reference/generateNegativeControlOutcomeCohorts.md)
  : Generate a set of negative control outcome cohorts

## Result Model Management

Functions for managing the results of running Cohort Generator via
`runCohortGeneration`

- [`createResultsDataModel()`](https://ohdsi.github.io/CohortGenerator/reference/createResultsDataModel.md)
  : Create the results data model tables on a database server.
- [`getDataMigrator()`](https://ohdsi.github.io/CohortGenerator/reference/getDataMigrator.md)
  : Get database migrations instance
- [`getResultsDataModelSpecifications()`](https://ohdsi.github.io/CohortGenerator/reference/getResultsDataModelSpecifications.md)
  : Get specifications for CohortGenerator results data model
- [`migrateDataModel()`](https://ohdsi.github.io/CohortGenerator/reference/migrateDataModel.md)
  : Migrate Data model
- [`uploadResults()`](https://ohdsi.github.io/CohortGenerator/reference/uploadResults.md)
  : Upload results to the database server.

## CSV File Helpers

Functions for reading and writing CSV files to ensure adherance to the
HADES standard when interfacing between R and SQL/File System:
<https://ohdsi.github.io/Hades/codeStyle.html#Interfacing_between_R_and_SQL>

- [`readCsv()`](https://ohdsi.github.io/CohortGenerator/reference/readCsv.md)
  : Used to read a .csv file
- [`writeCsv()`](https://ohdsi.github.io/CohortGenerator/reference/writeCsv.md)
  : Used to write a .csv file
- [`isCamelCase()`](https://ohdsi.github.io/CohortGenerator/reference/isCamelCase.md)
  : Used to check if a string is in lower camel case
- [`isSnakeCase()`](https://ohdsi.github.io/CohortGenerator/reference/isSnakeCase.md)
  : Used to check if a string is in snake case
- [`isFormattedForDatabaseUpload()`](https://ohdsi.github.io/CohortGenerator/reference/isFormattedForDatabaseUpload.md)
  : Is the data.frame formatted for uploading to a database?

## Record Keeping

Functions that support record keeping of tasks performed.
CohortGenerator uses these functions when running in incremental mode to
only generate cohorts when their definition has changed from a previous
run.

- [`getLastGeneratedCohortChecksums()`](https://ohdsi.github.io/CohortGenerator/reference/getLastGeneratedCohortChecksums.md)
  : Get last generated cohort checksums
- [`computeChecksum()`](https://ohdsi.github.io/CohortGenerator/reference/computeChecksum.md)
  : Computes the checksum for a value

## Cohort Sampling

Functions that support sampling a cohort. Please note this is only for
software development purposes and NOT for running studies.

- [`sampleCohortDefinitionSet()`](https://ohdsi.github.io/CohortGenerator/reference/sampleCohortDefinitionSet.md)
  : Sample Cohort Definition Set

## Sample Data

Sample data used in vignettes to showcase how to perform cohort subsets.

- [`omopCdmDrugExposure`](https://ohdsi.github.io/CohortGenerator/reference/omopCdmDrugExposure.md)
  : OMOP CDM Drug Exposure Sample Data
- [`omopCdmPerson`](https://ohdsi.github.io/CohortGenerator/reference/omopCdmPerson.md)
  : OMOP CDM Person Sample Data
