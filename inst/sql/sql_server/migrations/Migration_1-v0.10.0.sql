-- Database migrations for version 0.10.0
-- Adds missing database_id to cohort_censor_stats
-- Adds new table subset_definition to hold the subset definitions

ALTER TABLE @database_schema.@table_prefix@cohort_censor_stats ADD database_id VARCHAR;

CREATE TABLE @database_schema.@table_prefix@subset_definition (
    subset_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(subset_definition_id)
);
