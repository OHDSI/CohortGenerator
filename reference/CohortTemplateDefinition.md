# Class for automating the creation of bulk cohorts

Class for automating the creation of bulk cohorts

Class for automating the creation of bulk cohorts

## Details

This class provides a framework for automating the creation of bulk
cohorts by defining template SQL queries and associated callbacks to
execute them. This is useful when defining lots of exposure or outcomes
for cohorts that are very general in nature. For example, all RxNorm
ingredient cohorts, all ATC ingredient cohorts or all SNOMED condition
occurrences with \> x diagnosis codes.

These cohorts can then be subsetted with common cohort subset operations
such as limiting to specific age, gender, or observation criteria,
should this be excluded from the cohort definition. However, when
applying operations in bulk it may be more efficient to include such
definitions within the template sql itself.

This approach is also useful for cohorts that can not based on
ATLAS/CirceDefinitions alone.

CURRENTLY NOT SUPPORTED: \* Saving definitions that use runtime
arguments on a per cdm basis. This creates a challenge for running the
same cohort across different databases. Furthermore, saving information
within the CDM schema in a shared OHDSI study is not desirable.

## Active bindings

- `name`:

  name for this template definition that describes the cohorts it
  creation

- `sqlArgs`:

  optional arguments for sql

- `templateSql`:

  sql template

- `translateSql`:

  translate the sql for different platforms

- `references`:

  data.frame of name/id references for cohort template that aligns with
  cohort set

## Methods

### Public methods

- [`CohortTemplateDefinition$new()`](#method-CohortTemplateDefinition-new)

- [`CohortTemplateDefinition$executeTemplateSql()`](#method-CohortTemplateDefinition-executeTemplateSql)

- [`CohortTemplateDefinition$getTemplateReferences()`](#method-CohortTemplateDefinition-getTemplateReferences)

- [`CohortTemplateDefinition$getName()`](#method-CohortTemplateDefinition-getName)

- [`CohortTemplateDefinition$getId()`](#method-CohortTemplateDefinition-getId)

- [`CohortTemplateDefinition$getChecksum()`](#method-CohortTemplateDefinition-getChecksum)

- [`CohortTemplateDefinition$toList()`](#method-CohortTemplateDefinition-toList)

- [`CohortTemplateDefinition$toJson()`](#method-CohortTemplateDefinition-toJson)

- [`CohortTemplateDefinition$saveTemplate()`](#method-CohortTemplateDefinition-saveTemplate)

- [`CohortTemplateDefinition$clone()`](#method-CohortTemplateDefinition-clone)

------------------------------------------------------------------------

### Method `new()`

#### Usage

    CohortTemplateDefinition$new(settings)

#### Arguments

- `settings`:

  Settings of object to load seealso createCohortTemplateDefinition To
  alter the execution, override this function in a subclass. This
  translates and executes the sql of the cohort definition Note that
  calling this function will generate the cohorts but will not do so in
  an incremental manner. Checksums and timestamps will, however, be
  added to the generation table ever want to call this function outside
  of a testing environment. It is best practice to always use the
  standard runCohortGeneration/generateCohortSet pipeline to ensure
  validity of execution steps.

------------------------------------------------------------------------

### Method `executeTemplateSql()`

#### Usage

    CohortTemplateDefinition$executeTemplateSql(
      connection,
      cohortDatabaseSchema,
      cdmDatabaseSchema,
      cohortTableNames,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
    )

#### Arguments

- `connection`:

  An object of type `connection` as created using the
  [`connect`](https://ohdsi.github.io/DatabaseConnector/reference/connect.html)
  function in the DatabaseConnector package. Can be left NULL if
  `connectionDetails` is provided, in which case a new connection will
  be opened at the start of the function, and closed when the function
  finishes.

- `cohortDatabaseSchema`:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- `cohortDatabaseSchema`:

  Schema name where your cohort tables reside. Note that for SQL Server,
  this should include both the database and schema name, for example
  'scratch.dbo'.

- `cdmDatabaseSchema`:

  Schema name where your patient-level data in OMOP CDM format resides.
  Note that for SQL Server, this should include both the database and
  schema name, for example 'cdm_data.dbo'.

- `cohortTableNames`:

  The names of the cohort tables. See
  [`getCohortTableNames`](https://ohdsi.github.io/CohortGenerator/reference/getCohortTableNames.md)
  for more details.

- `vocabularyDatabaseSchema`:

  vocabulary database schema

- `tempEmulationSchema`:

  cdm temp emulation schema get template references data.frame

------------------------------------------------------------------------

### Method `getTemplateReferences()`

Returns data.frame of references get the name of the definition

#### Usage

    CohortTemplateDefinition$getTemplateReferences()

------------------------------------------------------------------------

### Method `getName()`

Name field get the generated id of the template definition

#### Usage

    CohortTemplateDefinition$getName()

------------------------------------------------------------------------

### Method `getId()`

this is not the cohort ids and is based off of the checksum of the
template definition get checksum

#### Usage

    CohortTemplateDefinition$getId()

------------------------------------------------------------------------

### Method `getChecksum()`

Get the hash of the definition (generated when class is instantiated) to
list

#### Usage

    CohortTemplateDefinition$getChecksum()

------------------------------------------------------------------------

### Method `toList()`

Used for serializing the definition to json

#### Usage

    CohortTemplateDefinition$toList()

------------------------------------------------------------------------

### Method `toJson()`

json serialized form of the template definition save to disk

#### Usage

    CohortTemplateDefinition$toJson()

------------------------------------------------------------------------

### Method `saveTemplate()`

Save object to specified json path

#### Usage

    CohortTemplateDefinition$saveTemplate(filePath)

#### Arguments

- `filePath`:

  File path to save json serialized from

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    CohortTemplateDefinition$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
