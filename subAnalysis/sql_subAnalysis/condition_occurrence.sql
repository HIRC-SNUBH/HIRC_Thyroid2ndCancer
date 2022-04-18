select s1.subject_id, s1.cohort_definition_id, co.condition_start_date, co.condition_concept_id
from (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1,
     @cdm_schema.condition_occurrence co
where s1.subject_id = co.person_id
  and (s1.cohort_start_date + 365-1 <= co.condition_start_date or s1.cohort_end_date >= co.condition_start_date)
  and co.condition_concept_id != 0;