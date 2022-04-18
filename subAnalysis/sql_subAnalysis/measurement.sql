select s1.subject_id, s1.cohort_start_date, m.measurement_date, m.value_as_number
from @cdm_schema.measurement m, (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid)) s1
where s1.subject_id = m.person_id and m.measurement_concept_id = 46236014;