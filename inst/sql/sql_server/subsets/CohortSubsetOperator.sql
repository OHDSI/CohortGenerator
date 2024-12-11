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
  -- AND Cohort lies within window criteria
  @cohort_window_logic
  GROUP BY T.subject_id, T.cohort_start_date, T.cohort_end_date
  HAVING COUNT (DISTINCT S.COHORT_DEFINITION_ID) >= @subset_length
) A
{@negate == '1'}?{
RIGHT JOIN @target_table B ON B.subject_id = A.subject_id 
  AND b.cohort_start_date = a.cohort_start_date
WHERE A.subject_id IS NULL
}
