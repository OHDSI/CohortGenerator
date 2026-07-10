# Cohort Subset Definition

Set of subset definitions pretty in print

## Active bindings

- `targetOutputPairs`:

  list of pairs of integers - (targetCohortId, outputCohortId)

- `subsetOperators`:

  list of subset operations

- `name`:

  name of definition

- `subsetCohortNameTemplate`:

  template string for formatting resulting cohort names

- `operatorNameConcatString`:

  string used when concatenating operator names together

- `definitionId`:

  numeric definition id

- `identifierExpression`:

  expression that can be evaluated from

## Methods

### Public methods

- [`CohortSubsetDefinition$print()`](#method-CohortSubsetDefinition-print)

- [`CohortSubsetDefinition$new()`](#method-CohortSubsetDefinition-initialize)

- [`CohortSubsetDefinition$toList()`](#method-CohortSubsetDefinition-toList)

- [`CohortSubsetDefinition$toJSON()`](#method-CohortSubsetDefinition-toJSON)

- [`CohortSubsetDefinition$addSubsetOperator()`](#method-CohortSubsetDefinition-addSubsetOperator)

- [`CohortSubsetDefinition$getSubsetQuery()`](#method-CohortSubsetDefinition-getSubsetQuery)

- [`CohortSubsetDefinition$getSubsetCohortName()`](#method-CohortSubsetDefinition-getSubsetCohortName)

- [`CohortSubsetDefinition$setTargetOutputPairs()`](#method-CohortSubsetDefinition-setTargetOutputPairs)

- [`CohortSubsetDefinition$getJsonFileName()`](#method-CohortSubsetDefinition-getJsonFileName)

- [`CohortSubsetDefinition$clone()`](#method-CohortSubsetDefinition-clone)

------------------------------------------------------------------------

### `CohortSubsetDefinition$print()`

#### Usage

    CohortSubsetDefinition$print(...)

#### Arguments

- `...`:

  further arguments passed to or from other methods.

------------------------------------------------------------------------

### `CohortSubsetDefinition$new()`

#### Usage

    CohortSubsetDefinition$new(definition = NULL)

#### Arguments

- `definition`:

  json or list representation of object to List

------------------------------------------------------------------------

### `CohortSubsetDefinition$toList()`

List representation of object to JSON

#### Usage

    CohortSubsetDefinition$toList()

------------------------------------------------------------------------

### `CohortSubsetDefinition$toJSON()`

json serialized representation of object add Subset Operator

#### Usage

    CohortSubsetDefinition$toJSON()

------------------------------------------------------------------------

### `CohortSubsetDefinition$addSubsetOperator()`

add subset to class - checks if equivalent id is present Will throw an
error if a matching ID is found but reference object is different

#### Usage

    CohortSubsetDefinition$addSubsetOperator(subsetOperator)

#### Arguments

- `subsetOperator`:

  a SubsetOperator instance

- `overwrite`:

  if a subset operator of the same ID is present, replace it with a new
  definition get query for a given target output pair

------------------------------------------------------------------------

### `CohortSubsetDefinition$getSubsetQuery()`

Returns vector of join, logic, having statements returned by subset
operations

#### Usage

    CohortSubsetDefinition$getSubsetQuery(targetOutputPair)

#### Arguments

- `targetOutputPair`:

  Target output pair Get name of an output cohort

------------------------------------------------------------------------

### `CohortSubsetDefinition$getSubsetCohortName()`

#### Usage

    CohortSubsetDefinition$getSubsetCohortName(
      cohortDefinitionSet,
      targetOutputPair
    )

#### Arguments

- `cohortDefinitionSet`:

  Cohort definition set containing base names

- `targetOutputPair`:

  Target output pair Set the targetOutputPairs to be added to a cohort
  definition set

------------------------------------------------------------------------

### `CohortSubsetDefinition$setTargetOutputPairs()`

#### Usage

    CohortSubsetDefinition$setTargetOutputPairs(targetIds)

#### Arguments

- `targetIds`:

  list of cohort ids to apply subsetting operations to Get json file
  name for subset definition in folder

------------------------------------------------------------------------

### `CohortSubsetDefinition$getJsonFileName()`

#### Usage

    CohortSubsetDefinition$getJsonFileName(
      subsetJsonFolder = "inst/cohort_subset_definitions/"
    )

#### Arguments

- `subsetJsonFolder`:

  path to folder to place file

------------------------------------------------------------------------

### `CohortSubsetDefinition$clone()`

The objects of this class are cloneable with this method.

#### Usage

    CohortSubsetDefinition$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
