DELETE FROM @output_database_schema.@output_table WHERE cohort_definition_id = @output_cohort_id;

-- SELECT ONLY SUBJECTS JOINED TO COHORT ID
INSERT INTO @output_database_schema.@output_table
SELECT
    @output_cohort_id as cohort_definition_id,
    iq.subject_id,
    iq.cohort_start_date,
    iq.cohort_end_date
FROM (
    -- NOTE: where DENSE_RANK < max(rst.rand_id) could prevent full table_scan?
    -- ORDER BY subject_id is deterministic
    -- DENSE_RANK means that DISTINCT or GROUP BY are not needed
    SELECT
        t.subject_id,
        t.cohort_start_date,
        t.cohort_end_date,
        DENSE_RANK() OVER (ORDER BY t.subject_id ) as u_id
    FROM @cohort_database_schema.@target_table t
    WHERE t.cohort_definition_id = @target_cohort_id
) iq
INNER JOIN @random_sample_table rst ON iq.u_id = rst.rand_id;

TRUNCATE TABLE @random_sample_table;
DROP TABLE @random_sample_table;
