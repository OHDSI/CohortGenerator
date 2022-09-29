-- NOTE: #nc_set is created by R before calling this SQL code
CREATE TABLE #Codesets (
  cohort_definition_id bigint NOT NULL,
  ancestor_concept_id int NOT NULL,
  concept_id int NOT NULL
)
;

{@detect_on_descendants == 'TRUE'} ? {
  INSERT INTO #Codesets (cohort_definition_id, ancestor_concept_id, concept_id)
  SELECT 
    n.cohort_id,
    ca.ancestor_concept_id, 
    ca.descendant_concept_id
  FROM @cdm_database_schema.CONCEPT_ANCESTOR ca
  INNER JOIN #nc_set n ON n.OUTCOME_CONCEPT_ID = ca.ancestor_concept_id
  ;
} : {
  INSERT INTO #Codesets (cohort_definition_id, ancestor_concept_id, concept_id)
  SELECT 
    n.cohort_id,
    c.concept_id, 
    c.concept_id
  FROM @cdm_database_schema.CONCEPT c
  INNER JOIN #nc_set n ON n.OUTCOME_CONCEPT_ID = c.concept_id
  ;
}

DELETE FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (
  SELECT DISTINCT ancestor_concept_id
  FROM #Codesets
)
;

INSERT INTO @cohort_database_schema.@cohort_table (
  subject_id,
  cohort_definition_id,
  cohort_start_date,
  cohort_end_date
)
SELECT
	s.subject_id,
	s.cohort_definition_id,
	s.cohort_start_date,
	s.cohort_start_date cohort_end_date
FROM (
  {@occurrence_type == 'first'}?{
     SELECT
      e.subject_id,
      e.cohort_definition_id,
      e.cohort_start_date,
      ROW_NUMBER() OVER (PARTITION BY e.subject_id, e.cohort_definition_id ORDER BY e.COHORT_START_DATE ASC) ordinal
    FROM (
  } : {}
  SELECT 
    d.person_id subject_id,
    c.cohort_definition_id,
    d.condition_start_date  cohort_start_date
  FROM @cdm_database_schema.condition_occurrence d
  INNER JOIN #Codesets c ON c.concept_id = d.condition_concept_id
  {@occurrence_type == 'first'}?{
  ) e
  }:{}
) s
{@occurrence_type == 'first'}?{WHERE s.ordinal = 1}:{}
;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;

TRUNCATE TABLE #nc_set;
DROP TABLE #nc_set;
