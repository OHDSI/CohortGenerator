WITH subset AS (
  SELECT 
    XREF.output_id as cohort_definition_id, 
    subject_id, 
    cohort_start_date, 
    cohort_end_date,
    row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date {@limit_to == 'firstEver' | @limit_to == 'earliestRemaining'}?{ASC}:{DESC}) ordinal
  FROM #target_output_xref XREF
  JOIN @subset_cohort_table c ON 
    XREF.TARGET_ID = c.cohort_definition_id
  {@limit_to == 'earliestRemaining' | @limit_to == 'latestRemaining'}?{
    JOIN @cdm_database_schema.observation_period op 
      ON c.subject_id = op.person_id
      AND c.cohort_start_date >= op.observation_period_start_date 
      AND c.cohort_start_date <= op.observation_period_end_date
    WHERE c.ordinal = 1 
      AND DATEDIFF(day, op.observation_period_start_date, c.cohort_start_date) >= @prior_time
      AND DATEDIFF(day, c.cohort_start_date, op.observation_period_end_date) >= @follow_up_time 
  }
)
SELECT 
  S.cohort_definition_id,
  S.subject_id,
  S.cohort_start_date,
  S.cohort_end_date
INTO @target_cohort_table
FROM subset c
{@limit_to == 'firstEver' | @limit_to == 'lastEver'}?{
  JOIN @cdm_database_schema.observation_period op 
    ON c.subject_id = op.person_id
    AND c.cohort_start_date >= op.observation_period_start_date 
    AND c.cohort_start_date <= op.observation_period_end_date
}
{@limit_to == 'firstEver' | @limit_to == 'lastEver' | @calendar_start_date != '1' | @calendar_end_date != '1'}?{
WHERE 
  1 = 1
}
{@limit_to == 'firstEver' | @limit_to == 'lastEver'}?{
  AND c.ordinal = 1 
  AND DATEDIFF(day, op.observation_period_start_date, c.cohort_start_date) >= @prior_time
  AND DATEDIFF(day, c.cohort_start_date, op.observation_period_end_date) >= @follow_up_time 
}
{@calendar_start_date != '1'}?{
  AND c.cohort_start_date >= DATEFROMPARTS(@calendar_start_date_year,@calendar_start_date_month,@calendar_start_date_day)
}
{@calendar_end_date != '1'}?{
  AND c.cohort_start_date <= DATEFROMPARTS(@calendar_end_date_year,@calendar_end_date_month,@calendar_end_date_day)
}
;

