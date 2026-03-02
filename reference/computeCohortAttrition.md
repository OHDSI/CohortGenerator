# Compute cohort attrition from inclusion rule statistics

Computes a sequential attrition table using the inclusion rule
statistics stored in the cohort statistics tables for Circe-based
cohorts. For each cohort definition, we report a base cohort entry count
(before inclusion rules) and then counts after applying the first `k`
inclusion rules in sequence.

Inclusion rule satisfaction is encoded as a bit mask in
`inclusionRuleMask`. For a rule sequence `i`, its bit value is `2^i`. A
row with `inclusionRuleMask` equal to the sum of the bits indicates
which rules were met. To compute the count after the first `k` rules, we
require all first-`k` bits to be set by checking
`bitwAnd(inclusionRuleMask, requiredMask) == requiredMask`, where
`requiredMask = 2^k - 1`.

Attrition is computed separately for each `modeId` present in
`cohortInclusionResult` (for example, person-level and event-level).

## Usage

``` r
computeCohortAttrition(cohortInclusionResult, cohortInclusion)
```

## Arguments

- cohortInclusionResult:

  A data.frame containing inclusion rule masks and counts, typically
  from the `cohortInclusionResultTable` with camelCase column names.
  Required columns: `databaseId`, `cohortDefinitionId`,
  `inclusionRuleMask`, `modeId`, `personCount`. You can obtain this via
  `getCohortStats(..., outputTables = "cohortInclusionResultTable")` or
  by querying the cohort results schema table created when stats are
  generated.

- cohortInclusion:

  A data.frame of inclusion rule metadata, typically from
  `cohortInclusionTable` with camelCase column names. Required columns:
  `cohortDefinitionId`, `ruleSequence`. You can obtain this via
  `getCohortStats(..., outputTables = "cohortInclusionTable")` or by
  querying the cohort results schema table created when stats are
  generated.

## Value

A data.frame with the following columns:

- `databaseId`: Database identifier.

- `cohortDefinitionId`: Cohort definition identifier.

- `modeId`: The mode identifier from `cohortInclusionResult`.

- `cohortEntry`: 1 for the base cohort entry count, 0 for rule rows.

- `ruleSequence`: Inclusion rule sequence (-1 for base row).

- `personCount`: Count after applying rules.
