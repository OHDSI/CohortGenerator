# Create createDemographicSubset Subset operator

Create createDemographicSubset Subset operator

## Usage

``` r
createDemographicSubsetOperator(
  name = NULL,
  ageMin = 0,
  ageMax = 99999,
  gender = NULL,
  race = NULL,
  ethnicity = NULL
)
```

## Arguments

- name:

  Optional char name

- ageMin:

  The minimum age

- ageMax:

  The maximum age

- gender:

  Gender demographics - concepts - 0, 8532, 8507, 0, "female", "male".
  Any string that is not "male" or "female" (case insensitive) is
  converted to gender concept 0.
  https://athena.ohdsi.org/search-terms/terms?standardConcept=Standard&domain=Gender&page=1&pageSize=15&query=
  Specific concept ids not in this set can be used but are not
  explicitly validated

- race:

  Race demographics - concept ID list

- ethnicity:

  Ethnicity demographics - concept ID list

## See also

Other subsets:
[`createCohortSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createCohortSubsetOperator.md),
[`createLimitSubsetOperator()`](https://ohdsi.github.io/CohortGenerator/reference/createLimitSubsetOperator.md)
