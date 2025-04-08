-- Database migrations for version 0.12.0
-- Adds checksum to cohort definition_table
{DEFAULT @cg_cohort_generation = cg_cohort_generation}
ALTER TABLE @database_schema.@table_prefix@cg_cohort_generation ADD checksum VARCHAR;