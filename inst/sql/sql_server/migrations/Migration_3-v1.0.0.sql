-- Database migrations for version 1.0.0
-- Adds concept set tables to schema
-- Adds is template field to cohort definition table
{DEFAULT @cg_concept_set = cg_concept_set}
{DEFAULT @cg_concept_set = cg_concept_set}
{DEFAULT @cg_cohort_concept_set = cg_cohort_concept_set}
{DEFAULT @cg_cohort_definition = cg_cohort_definition}
{DEFAULT @cg_cohort_template_definition = cg_cohort_template_definition}

CREATE TABLE @database_schema.@table_prefix@cg_concept_set(
    concept_set_id varchar NOT NULL,
    concept_id bigint NOT NULL,
    include_descendants int NOT NULL,
    is_excluded int NOT NULL,
    include_mapped int NOT NULL,
    primary key(concept_set_id, concept_id, include_descendants, is_excluded, include_mapped)
);

CREATE TABLE @database_schema.@table_prefix@cg_concept_set_name(
    concept_set_id varchar NOT NULL,
    concept_set_name varchar NOT NULL,
    primary key(concept_set_id, concept_set_name)
);

CREATE TABLE @database_schema.@table_prefix@cg_cohort_concept_set(
    concept_set_id varchar NOT NULL,
    cohort_definition_id bigint NOT NULL,
    primary key(concept_set_id, cohort_definition_id)
);

ALTER TABLE @database_schema.@table_prefix@cg_cohort_definition ADD COLUMN is_templated_cohort INT;


CREATE TABLE @database_schema.@table_prefix@cg_cohort_template_definition (
    template_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(template_definition_id)
);
