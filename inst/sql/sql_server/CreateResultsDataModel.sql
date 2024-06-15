CREATE TABLE @database_schema.@table_prefixcg_cohort_definition (
  	 cohort_definition_id BIGINT NOT NULL,
	 cohort_name VARCHAR,
	 description VARCHAR,
	 json TEXT,
	 sql_command TEXT,
	 subset_parent BIGINT,
	 is_subset INT,
	 subset_definition_id BIGINT,
	PRIMARY KEY(cohort_definition_id)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_generation (
  	 cohort_id BIGINT NOT NULL,
	 cohort_name VARCHAR,
	 generation_status VARCHAR,
	 start_time TIMESTAMP,
	 end_time TIMESTAMP,
	 database_id VARCHAR NOT NULL,
	PRIMARY KEY(cohort_id,database_id)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_inclusion (
  	 cohort_definition_id BIGINT NOT NULL,
	 rule_sequence INT NOT NULL,
	 name VARCHAR NOT NULL,
	 description VARCHAR,
	PRIMARY KEY(cohort_definition_id,rule_sequence,name)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_inc_result (
  	 database_id VARCHAR NOT NULL,
	 cohort_definition_id BIGINT NOT NULL,
	 inclusion_rule_mask INT NOT NULL,
	 person_count BIGINT NOT NULL,
	 mode_id INT NOT NULL,
	PRIMARY KEY(database_id,cohort_definition_id,inclusion_rule_mask,person_count,mode_id)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_inc_stats (
  	 database_id VARCHAR NOT NULL,
	 cohort_definition_id BIGINT NOT NULL,
	 rule_sequence INT NOT NULL,
	 person_count BIGINT NOT NULL,
	 gain_count BIGINT NOT NULL,
	 person_total BIGINT NOT NULL,
	 mode_id INT NOT NULL,
	PRIMARY KEY(database_id,cohort_definition_id,rule_sequence,person_count,gain_count,person_total,mode_id)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_summary_stats (
  	 database_id VARCHAR NOT NULL,
	 cohort_definition_id BIGINT NOT NULL,
	 base_count BIGINT NOT NULL,
	 final_count BIGINT NOT NULL,
	 mode_id INT NOT NULL,
	PRIMARY KEY(database_id,cohort_definition_id,base_count,final_count,mode_id)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_censor_stats (
  	 cohort_definition_id BIGINT NOT NULL,
	 lost_count BIGINT NOT NULL,
	PRIMARY KEY(cohort_definition_id,lost_count)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_count (
  	 database_id VARCHAR NOT NULL,
	 cohort_id BIGINT NOT NULL,
	 cohort_entries BIGINT NOT NULL,
	 cohort_subjects BIGINT NOT NULL,
	PRIMARY KEY(database_id,cohort_id,cohort_entries,cohort_subjects)
);
 
CREATE TABLE @database_schema.@table_prefixcg_cohort_count_neg_ctrl (
  	 database_id VARCHAR NOT NULL,
	 cohort_id BIGINT NOT NULL,
	 cohort_entries BIGINT NOT NULL,
	 cohort_subjects BIGINT NOT NULL,
	PRIMARY KEY(database_id,cohort_id,cohort_entries,cohort_subjects)
);
