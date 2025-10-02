-- Adds is template field to cohort definition table
{DEFAULT @cg_cohort_definition = cg_cohort_definition}
{DEFAULT @cg_cohort_template_definition = cg_cohort_template_definition}

ALTER TABLE @database_schema.@table_prefix@cg_cohort_definition ADD COLUMN is_templated_cohort INT;

CREATE TABLE @database_schema.@table_prefix@cg_cohort_template_definition (
    template_definition_id BIGINT,
    json varchar,
    PRIMARY KEY(template_definition_id)
);

UPDATE @database_schema.@table_prefix@cg_cohort_definition SET is_templated_cohort = 0;
