INSERT INTO @cohort_database_schema.@cohort_table
SELECT
    @output_cohort_id as cohort_definition_id,
    T.subject_id,
    T.cohort_start_date,
    T.cohort_end_date
FROM @target_table T;
