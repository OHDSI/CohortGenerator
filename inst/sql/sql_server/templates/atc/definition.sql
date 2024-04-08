{DEFAULT @prior_observation_period = 365}

DELETE FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id IN (SELECT COHORT_DEFINITION_ID FROM @cohort_database_schema.@atc_table);

DROP TABLE IF EXISTS #ingredient_eras;
-- First, create ingredient level cohorts
--HINT DISTRIBUTE_ON_KEY(person_id)
create table #ingredient_eras as
select
  et.cohort_definition_id
  , de1.concept_name
  , de1.person_id
  , de1.cohort_start_date
  , de1.cohort_end_date
from
  (
    select
        de0.person_id
        , atc_rxnorm.atc_concept_id  as drug_concept_id
        , atc_rxnorm.atc_concept_name as concept_name
        , de0.drug_era_start_date as cohort_start_date
        , de0.drug_era_end_date as cohort_end_date
        , row_number() over (partition by de0.person_id, atc_rxnorm.atc_concept_id order by de0.drug_era_start_date asc) row_num
  FROM @cdm_database_schema.drug_era de0
  inner join
      (
        SELECT c1.concept_id as descendant_concept_id, c1.concept_name as descendant_concept_name, c2.concept_id as atc_concept_id, c2.concept_name as atc_concept_name, c2.vocabulary_id as atc_id
        from @vocabulary_database_schema.concept c1
      	inner join @vocabulary_database_schema.concept_ancestor ca1 on c1.concept_id = ca1.descendant_concept_id
      	inner join @vocabulary_database_schema.concept c2 on ca1.ancestor_concept_id = c2.concept_id
      	where c1.vocabulary_id IN ('RxNorm') AND c2.concept_class_id = 'ATC 4th'
      ) atc_rxnorm
      on de0.drug_concept_id = atc_rxnorm.descendant_concept_id
  ) de1
INNER JOIN @cohort_database_schema.@atc_table et ON (et.concept_id = de1.drug_concept_id)
inner join @cdm_database_schema.observation_period op1
  on de1.person_id = op1.person_id
  and de1.cohort_start_date >= dateadd(dd,@prior_observation_period,op1.observation_period_start_date)
  and de1.cohort_start_date <= op1.observation_period_end_date
  and de1.row_num = 1
;

insert into @cohort_database_schema.@cohort_table
(
  cohort_definition_id
  , subject_id
  , cohort_start_date
  , cohort_end_date
)
select
  cohort_definition_id
  , person_id
  , cohort_start_date
  , cohort_end_date
from #ingredient_eras
;

TRUNCATE TABLE #ingredient_eras;
DROP TABLE #ingredient_eras;
