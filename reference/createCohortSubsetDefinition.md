# Create Subset Definition

Create subset definition from subset objects

## Usage

``` r
createCohortSubsetDefinition(
  name,
  definitionId,
  subsetOperators,
  identifierExpression = NULL,
  subsetCohortNameTemplate = "@baseCohortName - @subsetDefinitionName"
)
```

## Arguments

- name:

  Name of definition

- definitionId:

  Definition identifier

- subsetOperators:

  list of subsetOperator instances to apply

- identifierExpression:

  Expression (or string that converts to expression) that returns an id
  for an output cohort the default is dplyr::expr(targetId \* 1000 +
  definitionId)

- subsetCohortNameTemplate:

  SqlRender string template for formatting names of resulting subset
  cohorts Can use the variables @baseCohortName
  and @subsetDefinitionName. This is applied when adding the subset
  definition to a cohort definition set.
