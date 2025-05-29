-- Database migrations for version 0.12.0
-- Adds missing database_id to cg_cohort_censor_stats primary key
ALTER TABLE @database_schema.@table_prefixcg_cohort_censor_stats DROP CONSTRAINT cg_cohort_censor_stats_pkey;
ALTER TABLE @database_schema.@table_prefixcg_cohort_censor_stats ADD CONSTRAINT cg_cohort_censor_stats_pkey PRIMARY KEY (cohort_definition_id, lost_count, database_id);
