SELECT
  T.subject_id,
  T.cohort_start_date,
  T.cohort_end_date
FROM @target_table T

-- TO DO: observeration periods, first, last, all observation
