INSERT INTO @cohort_database_schema.@cohort_subset_attrition_table (
  cohort_definition_id,
  subset_definition_id,
  subset_parent_id,
  mode_id,
  cohort_entry,
  operator_sequence,
  count_value
)
SELECT
  @output_cohort_id,
  @subset_definition_id,
  @subset_parent_id,
  0,
  @cohort_entry,
  @operator_sequence,
  COUNT(*)
FROM @source_table;

INSERT INTO @cohort_database_schema.@cohort_subset_attrition_table (
  cohort_definition_id,
  subset_definition_id,
  subset_parent_id,
  mode_id,
  cohort_entry,
  operator_sequence,
  count_value
)
SELECT
  @output_cohort_id,
  @subset_definition_id,
  @subset_parent_id,
  1,
  @cohort_entry,
  @operator_sequence,
  COUNT(DISTINCT subject_id)
FROM @source_table;
