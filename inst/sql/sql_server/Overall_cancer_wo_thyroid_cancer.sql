CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (138708,134305,4079686,4189938,40483761,40481524,140352,4003184,4002497,4003188,4297355,4003835,4299149,40488896,4184976,4003021,4079282,4173963,4082311,434592,4173974,4112852,192273,4240913,4239729,442111,201824,193439,74606,379769,4093950,261255,24884,442108,4239871,376063,436948,4239873,77234,373982,373160,436936,4241026,442104,436062,136937,73723,24602,4241648,381252,36717482,4114356,198108,201817,132573,317814,433978,4239897,4242348,195498,442099,23731,135208,4156941,4243427,440973,315505,30679,200670,373166,438105,4003171,4001665,4031134,137813,4242361,373432,4154632,433987,435205,199764,4243445,444463,134304,4242814,75206,201823,30356,28115,4242816,197237,4243575,444457,200671,4114353,372272,4242974,434301,77517,74322,433981,438116,444459,138102,4242514,4242957,140966,141248,444445,136057,4243587,4242962,442105,201256,444453,194607,444450,200060,444449,4243593,138117,135767,4113124,196061,436366,4243596,438107,141249,440668,26060,439763,444444,198103,196377,78706,196364,195500,201244,438390,435506,72576,193431,193722,4112851,432860,4113129,40482072,4041800,140057,134603,4133599,4304484,4082487,40486171,4300704,4003830,4003832,4001328,4002356,4003831,4082627,138099,438383,4094548,4212994,40482893,4147411,4003833,4001329,4002357,4038845,4217892,4288751,441818,4001172,40483374,4002494,4038839,4038835,4041798,4038843,4038842,4308623,4003836,4001664,192854,40482847,434584,312846,4033893,140958,258981,375479,4038846,4044708,317510,4200649,198101,4031128,4033319,4001171,433997,4038841,4002358,132853,200662,4041104,4003834,4094544,4040379,4091913,4095589,4089860,4151263,4095592,141232,434590,133713,4153890,4149851,4032870,4111917,197506,40481902,77812,258084,4095018,4089777,201231,4092358,40491001,40482750,4095748,73153,436045,4095432,4092223,4091486,4089769,4091621,4097284,4094262,4094260,4097283,75756,133150,4091336,4092077,80665,4094872,4095168,4092513,4092515,4094409,443392,4110889,4116238,4114222,443389,4089530,4177242,4147164,4095312,4114198,25189,4177236,443397,4118989,4177112,4114221,4181485,4177113,40481901,4002496,4225982,4271013,4033297,4031757,4031106,4033296,4031763,4221679,4033837,4033839,4033891,4001666,4082463,321526,437233,4040380,138994,136950,40492268,140666,140967,140064,4266186,4180312,196365,4314207,198401,4313365,72578,4314634,4315940,4314637,4317409,4313379,442167,40492458,140357,193437,198402,22274,436080,199767,197518,442147,438997,4316507,200675,27516,4316887,4002341,4099495,22871,27835,194598,442140,201527,442141,4316995,4317685,4315953,40479707,135777,4317816,374313,4317822,4317020,201529,440971,439773,4317826,442129,4316746,79765,195793,4096519,4094118,432582,4038838,133419,198695,198082,4032866,4033318,4003693,372849,258375,79740,25748,438095,196051,140955,321234,440044,22839,133710,444224,28356,196645,432558,438691,4001170,192261,4003027,4002343,438090,133974,199747,378081,197803,436043,4002498,4003029,434881,133154,4216139,135214,4029188,4301668,432256,432254,193971,259748,4162115,198104,4162859,441225,436348,436643,432844,438982,436042,438080,80045,433143,4247719,441513,256633,194589,196360,434293,79749,193422,4162860,76914,4156114,380055,441806,4003674,4162253,4187851,4188545,4158563,4187850,432264,134579,432838,27235,4246029,432837,4247822,433149,436926,432848,441224,378696,4246808,197225,4003675,197500,255192,373151,375490,433975,442134,4003028,441800,42709931,434587,435752,441805,133420,4247238,80340,26638,140046,436358,40486896,374874,201801,201238,135750,441233,132258,435751,197806,438370,260336,437224,140950,438694,442131,440649,316644,438979,434577,439746,4311480,132832,137219,138351,195482,4247331,25486,192847,436913,198985,437501,441802,372567,436352,26052,438692,440345,434292,434289,4155171,201519,436640,440335,201518,4157454,193138,257503,4247836,197507,379756,137800,438693,4002340,4247336,256646,437805,4247842,438367,376918,432559,4246137,439404,432833,200051,134290,199754,433423,442122,196047,139753,200963,438086,434588,195483,4307721,4003694,4003179,4003175,28083,26361,24296,36715801,78093,436922,435474,435487,436344,439739,440339,200962,438089,192255,435190,438699,74582,195480,4311499,377811,433704,198091,438977,441520,79758,434880,435493,436635,139750,435755,136917,136915,434291,133147,192836,254282,376647,197808,436357,193719,136639,135491,134295,193418,437798,196044,259755,439738,441223,261514,432843,433976,433716,135476,438368,437498,31509,433709,440047,432257,435484,4312698,436054,435478,261236,252840,432260,201517,200054,76924,196049,436923,76349,200052,196359,45770892,434285,196048,438360,437220,133969,132565,138074,261808,195197,40481522,4003185,136949,4002495,4003186,4099508,40488812,4115271,193144,78097,4246451,4247962,192568,200959,439751,200348,44806773,254591,318096,442182,320342,434298,434875,373425,199752,72266,4147162,253717,4315806,196925,136354,198371,78987,432851,315497,136656,313430,4301666,195769,195770,4041799,4227653,4079683,4003182,197236,4098597)

) I
) C;


with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 11))
) C


-- End Condition Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P

-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

create table #inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);

with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
{0 != 0}?{
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),0)-1)
}
)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;

-- date offset strategy

select event_id, person_id, 
  case when DATEADD(day,1,start_date) > start_date then DATEADD(day,1,start_date) else start_date end as end_date
INTO #strategy_ends
from #included_events;


-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- End Date Strategy
SELECT event_id, person_id, end_date from #strategy_ends

),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;

{0 != 0}?{
-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM #qualified_events Q
		LEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #qualified_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from @results_database_schema.cohort_inclusion ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #qualified_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #qualified_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 0 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
WHERE ir.cohort_definition_id = @target_cohort_id
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select count_big(event_id) as total from #qualified_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
  where sr.mode_id = 0 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #best_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select ir.cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from @results_database_schema.cohort_inclusion ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #best_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #best_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 1 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule' 
WHERE ir.cohort_definition_id = @target_cohort_id
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select count_big(event_id) as total from #best_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from @results_database_schema.cohort_inclusion where cohort_definition_id = @target_cohort_id) RuleTotal
  where sr.mode_id = 1 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

-- BEGIN: Censored Stats

-- END: Censored Stats

TRUNCATE TABLE #best_events;
DROP TABLE #best_events;

}

TRUNCATE TABLE #strategy_ends;
DROP TABLE #strategy_ends;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
