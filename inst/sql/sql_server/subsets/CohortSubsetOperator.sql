{DEFAULT @cohort_database_schema = @cohort_database_schema}
{DEFAULT @cohort_table = @cohort_table}

SELECT
  {@negate == '1'}?{B}:{A}.subject_id, 
  {@negate == '1'}?{B}:{A}.cohort_start_date, 
  {@negate == '1'}?{B}:{A}.cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM (
  SELECT
    T.subject_id, 
    T.cohort_start_date, 
    T.cohort_end_date
  FROM @target_table T
  JOIN @cohort_database_schema.@cohort_table S ON T.subject_id = S.subject_id
  WHERE S.cohort_definition_id in (@cohort_ids)
    AND (S.cohort_start_date >= DATEADD(d, @start_window_start_day, T.@start_window_anchor) AND S.cohort_start_date <= DATEADD(d, @start_window_end_day, T.@start_window_anchor))
    AND (S.cohort_end_date >= DATEADD(d, @end_window_start_day, T.@end_window_anchor) and S.cohort_end_date <= DATEADD(d, @end_window_end_day, T.@end_window_anchor))
  GROUP BY T.subject_id, T.cohort_start_date, T.cohort_end_date
  HAVING COUNT (DISTINCT S.COHORT_DEFINITION_ID) >= @subset_length
) A
{@negate == '1'}?{
RIGHT JOIN @target_table B ON B.subject_id = A.subject_id
WHERE A.subject_id IS NULL
}
