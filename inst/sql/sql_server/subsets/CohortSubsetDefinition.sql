-- REQUIRED PARAMETERS:
--  @target_cohort_id
--  @output_cohort_id
--  @cohort_database_schema
--  @cohort_table
-- OPTIONAL PARAMETERS
{DEFAULT @join_statements = ''}
{DEFAULT @and_clauses = ''}
{DEFAULT @having_clauses = ''}

SELECT
    @output_cohort_id as cohort_definition_id
    t.subject_id,
    t.cohort_start_date,
    t.cohort_end_date
FROM @cohort_database_schema.@cohort_table t
@join_statements
WHERE t.cohort_definition_id IN (@target_cohort_id)
@and_clauses
GROUP BY t.subject_id, t.cohort_start_date, t.cohort_end_date
@having_clauses;
