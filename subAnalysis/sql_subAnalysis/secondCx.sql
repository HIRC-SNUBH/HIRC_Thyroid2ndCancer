with hematology as (
    select ca.descendant_concept_id as cancer_concept_id, 'hema' as sub_group
    from @cdm_schema.concept_ancestor ca
    where ancestor_concept_id in (4304484,4132554)
      and not exists (select 1 from (select descendant_concept_id from @cdm_schema.concept_ancestor where ancestor_concept_id = 4178976) tc where ca.descendant_concept_id = tc.descendant_concept_id)
      ),
    non_hematology as (
    select ca.descendant_concept_id as cancer_concept_id, 'non-hema' as sub_group
    from @cdm_schema.concept_ancestor ca
    where ancestor_concept_id in (433435,443392)
      and not exists (select 1 from (select descendant_concept_id from @cdm_schema.concept_ancestor where ancestor_concept_id = 4178976) tc where ca.descendant_concept_id = tc.descendant_concept_id)
      ),
    cohort_table as( 
    select s1.*, co.condition_start_date, condition_concept_id, c2.concept_name, c1.sub_group
    from @cdm_schema.condition_occurrence co
       , (select ct.cohort_definition_id
                ,ct.subject_id
                ,ct.cohort_start_date as risk_start_date 
          from @cohort_schema.@cohort_table ct 
		  where ct.cohort_definition_id in (@targetid, @comparatorid)) s1
        , hematology c1
        , @cdm_schema.concept c2
    where co.person_id = s1.subject_id
      and co.condition_start_date >= s1.risk_start_date
      and co.condition_concept_id != 0
      and co.condition_concept_id = c1.cancer_concept_id
      and co.condition_concept_id = c2.concept_id
    union all 
        select s1.*, co.condition_start_date, condition_concept_id, c2.concept_name, c1.sub_group
    from @cdm_schema.condition_occurrence co
       , (select ct.cohort_definition_id
                ,ct.subject_id
                ,ct.cohort_start_date as risk_start_date 
          from @cohort_schema.@cohort_table ct 
		  where ct.cohort_definition_id in (@targetid, @comparatorid)) s1
        , non_hematology c1
        , @cdm_schema.concept c2
    where co.person_id = s1.subject_id
      and co.condition_start_date >= s1.risk_start_date
      and co.condition_concept_id != 0
      and co.condition_concept_id = c1.cancer_concept_id
      and co.condition_concept_id = c2.concept_id
    ),
    temp as (
    select k1.*, rank() over(partition by k1.subject_id order by k1.condition_start_date, k1.concept_name asc) as rank
    from cohort_table k1
    )
select * from temp where rank = 1
;