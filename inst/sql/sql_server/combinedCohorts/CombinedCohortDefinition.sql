select subject_id, cohort_start_date, cohort_end_date
INTO #combined_cohort
FROM (
  @combined_cohort_query
) Q;


INSERT INTO @target_database_schema.@target_cohort_table
SELECT
    @output_cohort_id as cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
FROM #combined_cohort;

DROP TABLE #combined_cohort;
