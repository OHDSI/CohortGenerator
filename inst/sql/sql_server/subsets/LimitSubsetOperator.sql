SELECT
  c.subject_id,
  c.cohort_start_date,
  c.cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM (
  SELECT
    subject_id,
    cohort_start_date,
    cohort_end_date,
    row_number() over (partition by subject_id order by cohort_start_date {@limit_to == 'firstEver' | @limit_to == 'earliestRemaining'}?{ASC}:{DESC}) ordinal
  FROM @target_table t
  {(@limit_to == 'earliestRemaining' | @limit_to == 'latestRemaining') & @use_prior_fu_time}?{
    JOIN @cdm_database_schema.observation_period op
      ON t.subject_id = op.person_id
      AND t.cohort_start_date >= op.observation_period_start_date
      AND t.cohort_start_date <= op.observation_period_end_date
    WHERE DATEDIFF(day, op.observation_period_start_date, t.cohort_start_date) >= @prior_time
      AND DATEDIFF(day, t.cohort_start_date, op.observation_period_end_date) >= @follow_up_time
  }
) c
{(@limit_to == 'firstEver' | @limit_to == 'lastEver' | @limit_to == 'all') & @use_prior_fu_time}?{
  JOIN @cdm_database_schema.observation_period op
    ON c.subject_id = op.person_id
    AND c.cohort_start_date >= op.observation_period_start_date
    AND c.cohort_start_date <= op.observation_period_end_date
}
{@limit_to == 'firstEver' | @limit_to == 'lastEver' | @limit_to == 'earliestRemaining' | @limit_to == 'latestRemaining'}?{
 WHERE c.ordinal = 1
} : {
 WHERE 1 = 1
}
{(@limit_to == 'firstEver' | @limit_to == 'lastEver' | @limit_to == 'all') & @use_prior_fu_time}?{
  AND DATEDIFF(day, op.observation_period_start_date, c.cohort_start_date) >= @prior_time
  AND DATEDIFF(day, c.cohort_start_date, op.observation_period_end_date) >= @follow_up_time
}
{@calendar_start_date == '1'}?{
  AND c.cohort_start_date >= DATEFROMPARTS(@calendar_start_date_year,@calendar_start_date_month,@calendar_start_date_day)
}
{@calendar_end_date == '1'}?{
  AND c.cohort_start_date <= DATEFROMPARTS(@calendar_end_date_year,@calendar_end_date_month,@calendar_end_date_day)
}
