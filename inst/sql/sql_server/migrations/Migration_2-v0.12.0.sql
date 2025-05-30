-- Database migrations for version 0.12.0
-- Adds missing database_id to cg_cohort_censor_stats primary key
DROP TABLE IF EXISTS @database_schema.@table_prefixcg_cohort_censor_stats_temp;

CREATE TABLE @database_schema.@table_prefixcg_cohort_censor_stats_temp (
	 cohort_definition_id BIGINT NOT NULL,
	 lost_count BIGINT NOT NULL,
	 database_id VARCHAR
)
;

INSERT INTO @database_schema.@table_prefixcg_cohort_censor_stats_temp
SELECT
  cohort_definition_id,
  lost_count,
  database_id
FROM @database_schema.@table_prefixcg_cohort_censor_stats
;

DROP TABLE @database_schema.@table_prefixcg_cohort_censor_stats;

CREATE TABLE @database_schema.@table_prefixcg_cohort_censor_stats_temp (
	 cohort_definition_id BIGINT NOT NULL,
	 lost_count BIGINT NOT NULL,
	 database_id VARCHAR,
	 PRIMARY KEY(cohort_definition_id,lost_count,database_id)
)
;

INSERT INTO @database_schema.@table_prefixcg_cohort_censor_stats
SELECT
  cohort_definition_id,
  lost_count,
  database_id
FROM @database_schema.@table_prefixcg_cohort_censor_stats_temp
;

DROP TABLE @database_schema.@table_prefixcg_cohort_censor_stats_temp;
