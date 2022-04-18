select s1.*
from (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1, @cdm_schema.death d
where s1.subject_id = d.person_id;