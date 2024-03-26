DROP TABLE IF EXISTS @cohort_database_schema.@rx_norm_table;

CREATE TABLE @cohort_database_schema.@rx_norm_table
AS SELECT DISTINCT
    @identifier_expression AS COHORT_DEFINITION_ID,
    CONCEPT_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME) AS COHORT_NAME,
    CONCEPT_ID
FROM @vocabulary_database_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S');