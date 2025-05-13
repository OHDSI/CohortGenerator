{DEFAULT @prior_observation_period = 365}
{DEFAULT @merge_ingredient_eras = TRUE}
{DEFAULT @atc_level = 'ATC 4th'}
{DEFAULT @vocabulary_database_schema = @cdm_database_schema}


insert into @cohort_database_schema.@cohort_table
(
  cohort_definition_id
  , subject_id
  , cohort_start_date
  , cohort_end_date
)
{@merge_ingredient_eras} ? {
-- Definitions will either merge distinct ingredient eras into a single cohort definition or take the first exposure
-- As the ATC class member
select
	@identifier_expression AS COHORT_DEFINITION_ID,
	first_era.person_id as subject_id,
	first_era.start_date as cohort_start_date,
	first_era.end_date as cohort_end_date
from (
	select
	    -- change drug_concept_id to concept id to allow identifier expressions
		drug_concept_id as concept_id,
		person_id,
		start_date,
		end_date,
		era_num
	from (
		select
			drug_concept_id,
			person_id,
			start_date,
			end_date,
			row_number() over (partition by person_id, drug_concept_id order by start_date, end_date) as era_num
		from (
			select
				drug_concept_id,
				person_id,
				min(start_date) as start_date,
				dateadd(day,-30, max(end_date)) as end_date,
				count(*) as drug_exposure_count
			from (
				select
					drug_concept_id,
					person_id,
					start_date,
					end_date,
					sum(is_start) over (partition by drug_concept_id, person_id order by start_date, is_start desc rows unbounded preceding) as group_idx
				from (
					select
						drug_concept_id,
						drug_exposure_id,
						person_id,
						start_date,
						end_date,
						case
							when max(end_date) over (partition by drug_concept_id, person_id order by start_date rows between unbounded preceding and 1 preceding) >= start_date then 0
							else 1 end as is_start
					from (
						select
							drug_concept_id,
							drug_exposure_id,
							person_id,
							start_date,
							dateadd(day, 30, end_date) as end_date
						from (
							select
								c.concept_id as drug_concept_id,
								de.drug_exposure_id,
								de.person_id,
								de.drug_exposure_start_date as start_date,
								coalesce(de.drug_exposure_end_date, dateadd(day, de.days_supply, de.drug_exposure_start_date ), dateadd(day, 1, de.drug_exposure_start_date)) as end_date
							from @cdm_database_schema.drug_exposure de
							join @vocabulary_database_schema.concept_ancestor ca
								on de.drug_concept_id = ca.descendant_concept_id
							join @vocabulary_database_schema.concept c
								on ca.ancestor_concept_id = c.concept_id
							join @vocabulary_database_schema.concept c2
								on de.drug_concept_id = c2.concept_id
							where c.vocabulary_id = 'ATC' and c.concept_class_id = '@atc_level') de
					order by person_id, drug_concept_id, start_date) raw_data
				) starts
			) grp
			group by drug_concept_id, person_id, group_idx) eras
	) eras_counted
	where era_num = 1
) first_era
join @cdm_database_schema.observation_period op
	on first_era.person_id = op.person_id
	   and first_era.start_date >= dateadd(day, @prior_observation_period, op.observation_period_start_date)
	   and first_era.start_date <= op.observation_period_end_date
order by first_era.concept_id, first_era.person_id;
} : {
-- OLDER approach, just select first ingredient and don't merge eras
select
  @identifier_expression AS COHORT_DEFINITION_ID
  , person_id
  , cohort_start_date
  , cohort_end_date
from (
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
        , atc_rxnorm.atc_concept_id  as concept_id
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
      	where c1.vocabulary_id IN ('RxNorm') AND c2.concept_class_id = '@atc_level'
      ) atc_rxnorm
      on de0.drug_concept_id = atc_rxnorm.descendant_concept_id
  ) de1
inner join @cdm_database_schema.observation_period op1
  on de1.person_id = op1.person_id
  and de1.cohort_start_date >= dateadd(dd, @prior_observation_period, op1.observation_period_start_date)
  and de1.cohort_start_date <= op1.observation_period_end_date
  and de1.row_num = 1
);
}
