WITH subset AS (
  SELECT 
    XREF.output_id as cohort_definition_id, 
    T.subject_id, 
    T.cohort_start_date, 
    T.cohort_end_date
  FROM #target_output_xref XREF
  JOIN @subset_cohort_table T ON 
    XREF.TARGET_ID = t.cohort_definition_id
  JOIN @cohort_database_schema.@cohort_table S ON 
    XREF.SUBSET_ID = s.cohort_definition_id 
    AND t.subject_id = S.subject_id
  WHERE 
    (S.cohort_start_date >= DATEADD(d, @start_window_start_day, T.@start_window_anchor) AND S.cohort_start_date <= DATEADD(d, @start_window_end_day, T.@start_window_anchor))
    AND (S.cohort_end_date >= DATEADD(d, @end_window_start_day, T.@end_window_anchor) and S.cohort_end_date <= DATEADD(d, @end_window_end_day, T.@end_window_anchor))  
  GROUP BY T.subject_id, T.cohort_start_date, T.cohort_end_date
  HAVING COUNT (DISTINCT S.COHORT_DEFINITION_ID) >= @subset_length
)
SELECT 
  S.cohort_definition_id,
  S.subject_id,
  S.cohort_start_date,
  S.cohort_end_date
INTO @target_cohort_table
FROM subset S
{@negate == '1'}?{
  LEFT JOIN @cohort_database_schema.@cohort_table N ON S.subject_id = N.subject_id
  WHERE N.subject_id IS NULL
}
;