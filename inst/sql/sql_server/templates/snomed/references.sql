{DEFAULT @require_second_diagnosis = FALSE}
{DEFAULT @name_suffix = ''}
{DEFAULT @identifier_expression = concept_id * 1000}

SELECT
  DISTINCT
  @identifier_expression as cohort_id,
  CONCAT(c1.concept_name, '@name_suffix') as cohort_name
from (
--- CONCEPT ANCESTOR SUB QUERY START
    select
      ca1.ancestor_concept_id
    from @vocabulary_database_schema.concept_ancestor ca1
    inner join
    (
      select
        c1.concept_id
        , c1.concept_name
        , c1.vocabulary_id
        , c1.domain_id
      from @vocabulary_database_schema.concept c1
      inner join @vocabulary_database_schema.concept_ancestor ca1
        --clinical finding
        on ca1.ancestor_concept_id = 441840
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

--- CONCEPT ANCESTOR SUB QUERY END
) cag
inner join @vocabulary_database_schema.concept c1
  on cag.ancestor_concept_id = c1.concept_id