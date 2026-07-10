# Time Window For Cohort Subset Operator

Representation of a time window to use when subsetting a target cohort
with a subset cohort

## Active bindings

- `startDay`:

  Integer

- `endDay`:

  Integer

- `targetAnchor`:

  Boolean

- `subsetAnchor`:

  Boolean

- `negate`:

  Boolean

## Methods

### Public methods

- [`SubsetCohortWindow$toList()`](#method-SubsetCohortWindow-toList)

- [`SubsetCohortWindow$toJSON()`](#method-SubsetCohortWindow-toJSON)

- [`SubsetCohortWindow$isEqualTo()`](#method-SubsetCohortWindow-isEqualTo)

- [`SubsetCohortWindow$clone()`](#method-SubsetCohortWindow-clone)

------------------------------------------------------------------------

### `SubsetCohortWindow$toList()`

List representation of object To JSON

#### Usage

    SubsetCohortWindow$toList()

------------------------------------------------------------------------

### `SubsetCohortWindow$toJSON()`

json serialized representation of object Is Equal to

#### Usage

    SubsetCohortWindow$toJSON()

------------------------------------------------------------------------

### `SubsetCohortWindow$isEqualTo()`

Compare SubsetCohortWindow to another

#### Usage

    SubsetCohortWindow$isEqualTo(criteria)

#### Arguments

- `criteria`:

  SubsetCohortWindow instance

------------------------------------------------------------------------

### `SubsetCohortWindow$clone()`

The objects of this class are cloneable with this method.

#### Usage

    SubsetCohortWindow$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
