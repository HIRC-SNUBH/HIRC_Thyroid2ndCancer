select s1.*, vo.visit_concept_id, vo.visit_start_date, vo.visit_end_date
from (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1,
    @cdm_schema.visit_occurrence vo
where vo.person_id = s1.subject_id;