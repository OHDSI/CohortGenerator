{DEFAULT @cdm_database_schema = @cdm_database_schema}

SELECT
  T.subject_id,
  T.cohort_start_date,
  T.cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM @target_table T
JOIN @cdm_database_schema.person p ON T.subject_id = p.person_id
WHERE 1 = 1-- simplifies ternary logic
{@gender_concept_id != ''} ? {AND p.gender_concept_id IN (@gender_concept_id)}
{@race_concept_id != ''} ? {AND p.race_concept_id IN (@race_concept_id)}
{@ethnicity_concept_id != ''} ? {AND p.ethnicity_concept_id IN (@ethnicity_concept_id)}
{@age_min != ''} ? {AND YEAR(T.cohort_start_date) - p.year_of_birth >= @age_min}
{@age_max != ''} ? {AND YEAR(T.cohort_start_date) - p.year_of_birth <= @age_max}
