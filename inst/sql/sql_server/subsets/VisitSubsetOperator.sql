SELECT
  c.subject_id,
  c.cohort_start_date,
  c.cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM (
  SELECT
    t.subject_id,
    t.cohort_start_date,
    t.cohort_end_date
  FROM @target_table t
  JOIN @cdm_database_schema.visit_occurrence vo on t.subject_id = vo.person_id
  where vo.visit_concept_id in (@visit_concept_ids)
    AND t.cohort_start_date >= vo.visit_start_date
    AND t.cohort_start_date <= vo.visit_end_date
) c
