{DEFAULT name_suffix = ''}

SELECT DISTINCT
    @identifier_expression AS cohort_id,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME, '@name_suffix') AS cohort_name
FROM @vocabulary_database_schema.concept
WHERE (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC')