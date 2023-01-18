{DEFAULT @cohort_database_schema = @cohort_database_schema}
{DEFAULT @cohort_table = @cohort_table}

SELECT
  T.subject_id, 
  T.cohort_start_date, 
  T.cohort_end_date
FROM @target_table T
JOIN @cohort_database_schema.@cohort_table S ON T.subject_id = S.subject_id
WHERE 
  (S.cohort_start_date >= DATEADD(d, @start_window_start_day, T.@start_window_anchor) AND S.cohort_start_date <= DATEADD(d, @start_window_end_day, T.@start_window_anchor))
  AND (S.cohort_end_date >= DATEADD(d, @end_window_start_day, T.@end_window_anchor) and S.cohort_end_date <= DATEADD(d, @end_window_end_day, T.@end_window_anchor))  
GROUP BY T.subject_id, T.cohort_start_date, T.cohort_end_date
HAVING COUNT (DISTINCT S.COHORT_DEFINITION_ID) >= @subset_length