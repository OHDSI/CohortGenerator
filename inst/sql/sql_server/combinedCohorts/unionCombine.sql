select subject_id, min(cohort_start_date) as cohort_start_date, max(cohort_end_date) as cohort_end_date
from (
  select subject_id, cohort_start_date, cohort_end_date, sum(is_start) over (partition by subject_id order by cohort_start_date, is_start desc rows unbounded preceding) group_idx
  from (
    select subject_id, cohort_start_date, cohort_end_date, 
    case when max(cohort_end_date) over (partition by subject_id order by cohort_start_date rows between unbounded preceding and 1 preceding) >= cohort_start_date then 0 else 1 end is_start
    from (
      select subject_id, cohort_start_date, cohort_end_date
      from @target_database_schema.@target_cohort_table
      WHERE cohort_definition_id in (@target_cohort_ids)
    ) CR
  ) ST
) GR
group by subject_id, group_idx