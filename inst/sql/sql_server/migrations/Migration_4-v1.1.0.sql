-- Database migrations for version 1.1.0
-- Adds cg_cohort_attrition to store sequential attrition derived from inclusion stats
-- Adds cg_cohort_subset_attrition to store sequential attrition for cohort subset operators
{DEFAULT @cg_cohort_attrition = cg_cohort_attrition}
{DEFAULT @cg_cohort_subset_attrition = cg_cohort_subset_attrition}

CREATE TABLE @database_schema.@table_prefix@cg_cohort_attrition (
    database_id VARCHAR NOT NULL,
    cohort_definition_id BIGINT NOT NULL,
    mode_id INT NOT NULL,
    cohort_entry INT NOT NULL,
    rule_sequence INT NULL,
    person_count BIGINT NOT NULL,
	PRIMARY KEY(database_id,cohort_definition_id,mode_id,cohort_entry,rule_sequence)
);

CREATE TABLE @database_schema.@table_prefix@cg_cohort_subset_attrition (
    database_id VARCHAR NOT NULL,
    cohort_definition_id BIGINT NOT NULL,
    subset_definition_id BIGINT NOT NULL,
    subset_parent_id BIGINT NOT NULL,
    mode_id INT NOT NULL,
    cohort_entry INT NOT NULL,
    operator_sequence INT NOT NULL,
    count_value BIGINT NOT NULL,
	PRIMARY KEY(database_id,cohort_definition_id,subset_definition_id,subset_parent_id,mode_id,cohort_entry,operator_sequence)
);
