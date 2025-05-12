SELECT DISTINCT
    @identifier_expression AS COHORT_DEFINITION_ID,
    @identifier_expression AS SUBSET_PARENT,
    CONCEPT_NAME AS CONCEPT_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME) AS COHORT_NAME,
    CONCEPT_ID
FROM @vocabulary_database_schema.concept
WHERE (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC')