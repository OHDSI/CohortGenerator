WITH subset AS (
  SELECT 
    XREF.output_id as cohort_definition_id, 
    subject_id, 
    cohort_start_date, 
    cohort_end_date
  FROM #target_output_xref XREF
  JOIN @subset_cohort_table c ON 
    XREF.TARGET_ID = c.cohort_definition_id
)
SELECT 
  S.cohort_definition_id,
  S.subject_id,
  S.cohort_start_date,
  S.cohort_end_date
INTO @target_cohort_table
FROM subset c
JOIN @cdm_database_schema.PERSON p ON p.PERSON_ID = c.PERSON_ID
WHERE YEAR(c.cohort_start_date) - p.year_of_birth >= @age_min
  AND YEAR(c.cohort_start_date) - p.year_of_birth <= @age_max
{@gender != ''}?{
  AND p.gender_concept_id IN (@gender)
}
{@race != ''}?{
  AND p.race_concept_id IN (@race)
}
{@race != ''}?{
  AND p.ethnicity_concept_id IN (@ethnicity)
}
;
