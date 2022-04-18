select s1.subject_id, s1.cohort_definition_id, de.drug_era_start_date, de.drug_concept_id
from (select * from @cohort_schema.@cohort_table ct where ct.cohort_definition_id in (@targetid, @comparatorid)) s1,
     @cdm_schema.drug_era de,
     (
		select concept_id
		from @cdm_voca.concept
		where concept_class_id = 'Ingredient'
		  and vocabulary_id like 'RxNorm%'
	  ) dc
where s1.subject_id = de.person_id
  and (s1.cohort_start_date+365-1 <= de.drug_era_start_date or s1.cohort_end_date >= de.drug_era_start_date)
  and de.drug_concept_id != 0
  and dc.concept_id = de.drug_concept_id;
