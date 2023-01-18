DELETE FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (
  SELECT DISTINCT OUTPUT_ID 
  FROM #target_output_xref
)
;

INSERT INTO @cohort_database_schema.@cohort_table (
  cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
)
SELECT
  cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
FROM @target_cohort_table
;