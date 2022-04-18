select s1.*, op.observation_period_start_date, op.observation_period_end_date 
from (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1
	, @cdm_schema.observation_period op 
where s1.subject_id = op.person_id;