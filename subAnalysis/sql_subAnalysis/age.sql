 select subject_id, YEAR(s1.cohort_start_date)-p.year_of_birth+1 as age
 from @cdm_schema.person p,(select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1
 where p.person_id = s1.subject_id;