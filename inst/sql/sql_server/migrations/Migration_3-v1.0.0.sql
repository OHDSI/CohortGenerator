-- Adds is template field to cohort definition table
{DEFAULT @cg_cohort_definition = cg_cohort_definition}
{DEFAULT @cg_cohort_template_definition = cg_cohort_template_definition}
{DEFAULT @cg_cohort_template_link = cg_cohort_template_link}

-- Not necessary with normalization below but does allow faster performance for some web setups
ALTER TABLE @database_schema.@table_prefix@cg_cohort_definition ADD COLUMN is_templated_cohort INT;

CREATE TABLE @database_schema.@table_prefix@cg_cohort_template_definition (
    template_definition_id VARCHAR,
    -- Note that name and sql (as well as cohort ids) are all contained in the json
    -- This could create a referential integrity issue
    template_name VARCHAR,
    template_sql VARCHAR,
    json varchar,
    PRIMARY KEY(template_definition_id)
);

CREATE TABLE @database_schema.@table_prefix@cg_cohort_template_link (
    template_definition_id VARCHAR,
    cohort_definition_id BIGINT,
    PRIMARY KEY (template_definition_id, cohort_definition_id)
);

UPDATE @database_schema.@table_prefix@cg_cohort_definition SET is_templated_cohort = 0;
