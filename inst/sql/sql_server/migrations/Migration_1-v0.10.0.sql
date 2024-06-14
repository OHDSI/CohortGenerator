-- Database migrations for version 0.10.0
-- Adds missing database_id to cg_cohort_censor_stats
-- Adds new table cg_cohort_subset_definition to hold the subset definitions
ALTER TABLE @database_schema.@table_prefixcg_cohort_censor_stats ADD database_id VARCHAR;

CREATE TABLE @database_schema.@table_prefixcg_cohort_subset_definition (
    subset_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(subset_definition_id)
);
