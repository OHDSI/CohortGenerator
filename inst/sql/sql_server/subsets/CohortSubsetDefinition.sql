-- REQUIRED PARAMETERS:
--  @target_cohort_id
--  @output_cohort_id

-- OPTIONAL PARAMETERS
{DEFAULT @join_statements = ''}
{DEFAULT @logic_clauses = ''}
{DEFAULT @having_clauses = ''}

-- EXECUTION TIME PARAMETERS
{DEFAULT @cohort_database_schema = @cohort_database_schema }
{DEFAULT @cohort_table = @cohort_table }

SELECT
    @output_cohort_id as cohort_definition_id
    T.subject_id,
    T.cohort_start_date,
    T.cohort_end_date
FROM @cohort_database_schema.@cohort_table T
@join_statements
WHERE T.cohort_definition_id IN (@target_cohort_id)
@logic_clauses
GROUP BY T.subject_id, T.cohort_start_date, T.cohort_end_date
@having_clauses