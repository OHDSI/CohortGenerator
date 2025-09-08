{DEFAULT name_suffix = ''}

SELECT DISTINCT
    @identifier_expression AS COHORT_ID,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME, '@name_suffix') AS COHORT_NAME
FROM @vocabulary_database_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')