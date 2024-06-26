-- Database migrations for version 0.10.0
-- Adds missing database_id to cg_cohort_censor_stats
-- Adds new table cg_cohort_subset_definition to hold the subset definitions
-- Adds new table cg_cohort_definition_neg_ctrl to hold the negative control outcomes cohort definitions
ALTER TABLE @database_schema.@table_prefixcg_cohort_censor_stats ADD database_id VARCHAR;

CREATE TABLE @database_schema.@table_prefixcg_cohort_subset_definition (
    subset_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(subset_definition_id)
);

CREATE TABLE @database_schema.@table_prefixcg_cohort_definition_neg_ctrl (
    cohort_id BIGINT,
    outcome_concept_id BIGINT,
    cohort_name varchar,
    occurrence_type varchar,
    detect_on_descendants int,
    PRIMARY KEY(cohort_id)
);
