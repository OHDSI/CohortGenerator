{DEFAULT @require_second_diagnosis = FALSE}
-- Create outcome cohort definitions
create table #cpt_anc_grp as
select
  ca1.ancestor_concept_id
  , ca1.descendant_concept_id
from @vocabulary_schema.concept_ancestor ca1
inner join
(
  select
    c1.concept_id
    , c1.concept_name
    , c1.vocabulary_id
    , c1.domain_id
  from @vocabulary_database_schema.concept c1
  inner join @vocabulary_database_schema.concept_ancestor ca1
    on ca1.ancestor_concept_id = 441840 /* clinical finding */
    and c1.concept_id = ca1.descendant_concept_id
  where
  (
    ca1.min_levels_of_separation > 2
  	or c1.concept_id in (433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 4145627, 434157, 433778, 258449, 313878)
  )
  and c1.concept_name not like '%finding'
  and c1.concept_name not like 'disorder of%'
  and c1.concept_name not like 'finding of%'
  and c1.concept_name not like 'disease of%'
  and c1.concept_name not like 'injury of%'
  and c1.concept_name not like '%by site'
  and c1.concept_name not like '%by body site'
  and c1.concept_name not like '%by mechanism'
  and c1.concept_name not like '%of body region'
  and c1.concept_name not like '%of anatomical site'
  and c1.concept_name not like '%of specific body structure%'
  and c1.domain_id = 'Condition'
) t1
  on ca1.ancestor_concept_id = t1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @cohort_database_schema.@conditions_table
( cohort_definition_id,
  cohort_definition_name
  ,	short_name
  , concept_id
)
select
  DISTINCT
  @identifier_expression as cohort_definition_id,
  'outcome of ' + c1.concept_name + ' - first occurence of diagnosis' {@require_second_diagnosis} ? {' with 2 diagnosis codes '} as cohort_definition_name
  , ' outcome of ' + c1.concept_name {@require_second_diagnosis} ? {+ ' requiring 2 DX'} as short_name
  ,	c1.concept_id as concept_id
from
#cpt_anc_grp ca1
inner join @vocabulary_database_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;