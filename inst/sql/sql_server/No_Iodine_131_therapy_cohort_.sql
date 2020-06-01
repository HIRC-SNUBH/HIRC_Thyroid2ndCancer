CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4036252)

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (45571576,45600572,45537873,45542667,45561880,45600565,45586071,45581256,45557044,45557042,45537880,45571572,45552312,45581250,45586051,45561858,45561869,45571571,45586056,45566646,45590945,45755352,45561882,45600564,45576367,45605333,45590950,45605300,45576353,45600549,45600561,45581252,45571566,45537871,45571549,45595698,45532946,45542638,45590936,45552299,45605303,45557041,45605328,45581265,45576344,45581232,45571555,45586059,45600568,45561805,45600502,45581218,45556984,45571511,45561806,45566591,45556985,45557048,45586073,45586044,45571545,45547532,45537864,45586062,45542663,45755345,45537881,45590962,45561870,45600573,45571501,45585988,45590921,45755315,45571487,45552269,45755318,45595650,45532930,45571518,45595645,45537825,45576305,45586016,45755339,45585982,45590880,45581200,45755317,45566575,45600516,45755314,45595642,45581197,45532897,45547561,45557007,45755316,45537792,45600480,45556963,45581203,45755340,45755313,45605266,45590924,45532935,45576337,45537838,45581227,45571534,45547520,45571535,45556998,45571488,45537827,45576307,45576296,45566569,45566612,45552258,45542624,45542614,45595635,45547484,45576298,45566610,45595660,45576301,45605281,45547509,45561791,45542587,45552268,45561808,45585985,45537836,45532893,45557000,45605264,45561784,45585989,45600518,45537826,45576328,45600512,45600482,45595641,45552277,45552279,45566577,45576306,45552248,45532891,45537800,45537791,45566570,45571527,45547479,45556966,45532934,45595640,45595638,45600485,45600509,45566578,45576335,45571528,45556967,45571526,45547481,45595649,45576312,45566583,45542579,45537835,45571532,45542582,45547505,45552264,45542592,45542586,45556964,45552280,45590919,45586025,45566601,45547516,45595682,45561828,45542621,45600481,45547507,45600476,45557006,45605283,45566604,45532896,45561790,45590875,45571529,45552261,45537817,45576320,45571515,45556989,45537818,45571514,45542611,45537831,45605278,45571533,45542626,45605282,45552246,45537829,45576295,45552245,45595670,45547519,45547485,45566605,45595674,45590883,45576329,45556958,45581198,45595633,45552249,45590918,45542616,45566609,45547478,45557008,45566580,45581201,45571524,45547508,45600487,45542585,45585981,45605258,45590881,45537799,45561789,45542588,45542584,45600484,45576309,45571496,45532898,45547482,45532914,45566576,45586015,45556997,45542618,45556972,45595644,45595634,45586020,45552244,45566574,45600483,45561780,45537786,45556959,45552247,45537795,45581207,45561781,45552242,45605263,45547480,45576324,45581221,45552250,45566572,45576333,45595652,45542596,45556976,45566585,45542597,45590887,45547491,45537804,45532902,45590902,45571500,45537798,45576304,45537801,45590922,45532929,45556971,45595647,45595639,45556961,45595671,45566579,45576300,45566598,45595683,45571489,45561829,45537837,45566613,45595636,45605287,45556970,45571492,45547483,45552266,45532922,45605262,45581202,45532924,45556975,45571498,45532931,45586024,45532913,45542590,45552263,45600486,45581220,45566606,45590882,45542620,45566608,45537788,45595648,45590877,45595643,45581199,45600479,45556962,45566611,45605260,45590879,45542583,45552243,45556960,45537796,45590912,45556986,45561792,45556974,45600503,45581217,45532895,45576303,45566571,45585983,45557005,45547506,45537805,45600475,45537797,45556969,45552265,45542625,45552272,45561827,45585991,45576331,45556987,45552257,45537815,45595662,45581219,45537816,45576318,45605273,45556988,45605261,45590923,45532933,45605265,45566617,45537793,45571495,45590915,45561785,45595637,45590876,45556999,45585984,45532894,45600520,45561782,45605274,45595651,45600514,45532900,45576332,45552251,45532901,45571491,45600492,45532903,45581212,45566589,45585999,45532906,45581215,45600496,45561788,45537789,45542610,45547512,45542589,45581226,45552275,45576308,45556965,45571490,45566581,45600477,45542581,45605259,45561793,45556968,45600521,45532918,45590878,45537787,45600478,45566573,45576299,45585986,45600519,45532917,45605257,45576294,45595646,45585990,45576302,45561811,45561816,45552271,45552270,45532925,45537830,45605280,45542580,45532892,45605256,45537803,45576297,45595669,45561786,45566652,45542646,45537879,45755349,45595711,45755344,45542659,45537814,45576317,45586007,45595658,45571510,45556983,45566622,45755343,45547558,45576370,45542670,45590969,45600562,45755347,45605312,45600571,45571570,45561878,45537876,45586030,45547530,45561867,45542649,45755342,45595727,45547547,45552293,45532957,45581262,45755351,45561874,45552309,45537806,45561857,45581258,45600566,45576359,45595725,45581264,45755346,45595715,45600546,45571560,45552310,45590961,45542661,45581254,45586074,45561830,45571538,45542627,45576338,45595685,45557012,45557014,45590925,45557013,45590926,45552284,45571540,45561831,45547523,45595687,45595688,45537839,45532936,45576339,45561832,45586028,45537841,45547524,45532938,45552282,45755341,45547522,45571539,45595686,45600529,45590939,45586061,45755329,45755348,45755350,45532956,45595708,45542651,45755330,45605307,45581263,45557037,45566620)

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 12 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4082277,4200221,4149106,4030107,4122303,4073199,4030039)

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 14 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (45571576,45600572,45537873,45542667,45561880,45600565,45586071,45581256,45557044,45557042,45537880,45571572,45552312,45581250,45586051,45561858,45561869,45571571,45586056,45537894,45586088,45537895,45595736,45561897,45561898,45561899,45566667,45571601,45537896,45581285,45566683,45532979,45537891,45755338,45590978,45557068,45595751,45561901,45755335,45586083,45547576,45576390,45581296,45566664,45755363,45571606,45755337,45542696,45547589,45576391,45566662,45591002,45581281,45600580,45581308,45590994,45552334,45561896,45595752,45600591,45581303,45605345,45586086,45561909,45547594,45571610,45566680,45552345,45576377,45576386,45571602,45561902,45557061,45532975,45566670,45547581,45537899,45552335,45590996,45537905,45591001,45571597,45542682,45605353,45576379,45566678,45581283,45547593,45595742,45586085,45561894,45576376,45552328,45542684,45542695,45547591,45552329,45542680,45561892,45571599,45566666,45581307,45537890,45561907,45542683,45581302,45586090,45605348,45590983,45581289,45581284,45561891,45605343,45566682,45552325,45552327,45581280,45547588,45537889,45581305,45600587,45561895,45557073,45557074,45600589,45605344,45586099,45571596,45600582,45557069,45600592,45557059,45571595,45576392,45581304,45581306,45561905,45595733,45595734,45590993,45571598,45590997,45590984,45532971,45547578,45561906,45537892,45552331,45552326,45552339,45600585,45576382,45581295,45595740,45595741,45581294,45586094,45557064,45537906,45576389,45586087,45552341,45547577,45586084,45542681,45557057,45581282,45537903,45581297,45581298,45557067,45566677,45557058,45547586,45566646,45590945,45532967,45537885,45581278,45755332,45755331,45581271,45755333,45755334,45557056,45590971,45547575,45586076,45600579,45566656,45557055,45586075,45537882,45600578,45571590,45571589,45571592,45547565,45557049,45547564,45571581,45552323,45605342,45552319,45571591,45566660,45557050,45542671,45547574,45586082,45566653,45566654,45605337,45590973,45561888,45605338,45590975,45586080,45557054,45581275,45576373,45547572,45590976,45532966,45552324,45571582,45581279,45552322,42616301,45755352,45561882,45600564,45576367,45605333,45600598,45590950,45605300,45576353,45600549,45600561,45576400,45581252,45571566,45537871,45571549,45595698,45532946,45542638,45590936,45552299,45605303,45755336,45586089,45557041,45605328,45537914,45581265,45576344,45581232,45571555,45586059,45576374,45547585,45600568,45561805,45600502,45581218,45556984,45571511,45561806,45566591,45556985,45571605,45542693,45557048,45586073,45571586,45590982,45586044,45571545,45547532,45537864,45586062,45542663,45755345,45537881,45590962,45561870,45600573,45571501,45585988,45590921,45755315,45571487,45552269,45755318,45595650,45532930,45571518,45595645,45537825,45576305,45586016,45755339,45585982,45590880,45581200,45755317,45566575,45600516,45755314,45595642,45581197,45532897,45547561,45557007,45755316,45537792,45600480,45556963,45581203,45755340,45755313,45605266,45590924,45532935,45576337,45537838,45581227,45571534,45547520,45571535,45556998,45571488,45537827,45576307,45576296,45566569,45566612,45552258,45542624,45542614,45595635,45547484,45576298,45566610,45595660,45576301,45605281,45547509,45561791,45542587,45552268,45561808,45585985,45537836,45532893,45557000,45605264,45561784,45585989,45600518,45537826,45576328,45600512,45557017,45566620,45547529,45600482,45595641,45552277,45552279,45566577,45576306,45552248,45532891,45537800,45537791,45566570,45571527,45547479,45556966,45532934,45595640,45595638,45600485,45600509,45566578,45576335,45571528,45556967,45571526,45547481,45595649,45576312,45566583,45542579,45537835,45571532,45542582,45547505,45552264,45542592,45542586,45556964,45552280,45590919,45586025,45566601,45547516,45595682,45561828,45542621,45600481,45547507,45600476,45557006,45605283,45566604,45532896,45561790,45590875,45571529,45552261,45537817,45576320,45571515,45556989,45537818,45571514,45542611,45537831,45605278,45571533,45542626,45605282,45552246,45537829,45576295,45552245,45595670,45547519,45547485,45566605,45595674,45590883,45576329,45556958,45581198,45595633,45552249,45590918,45542616,45566609,45547478,45557008,45566580,45581201,45571524,45547508,45600487,45542585,45585981,45605258,45590881,45537799,45561789,45542588,45542584,45600484,45576309,45571496,45532898,45547482,45532914,45566576,45586015,45556997,45542618,45556972,45595644,45595634,45586020,45552244,45566574,45600483,45561780,45537786,45556959,45552247,45537795,45581207,45561781,45552242,45605263,45547480,45576324,45581221,45552250,45566572,45576333,45595652,45542596,45556976,45566585,45542597,45590887,45547491,45537804,45532902,45590902,45571500,45537798,45576304,45537801,45590922,45532929,45556971,45595647,45595639,45556961,45595671,45566579,45576300,45566598,45595683,45571489,45561829,45537837,45566613,45595636,45605287,45556970,45571492,45547483,45552266,45532922,45605262,45581202,45532924,45556975,45571498,45532931,45586024,45532913,45542590,45552263,45600486,45581220,45566606,45590882,45542620,45566608,45537788,45595648,45590877,45595643,45581199,45600479,45556962,45566611,45605260,45590879,45542583,45552243,45556960,45537796,45590912,45556986,45561792,45556974,45600503,45581217,45532895,45576303,45566571,45585983,45557005,45547506,45537805,45600475,45537797,45556969,45552265,45542625,45552272,45561827,45585991,45576331,45556987,45552257,45537815,45595662,45581219,45537816,45576318,45605273,45556988,45605261,45590923,45532933,45605265,45566617,45537793,45571495,45590915,45561785,45595637,45590876,45556999,45585984,45532894,45600520,45561782,45605274,45595651,45600514,45532900,45576332,45552251,45532901,45571491,45600492,45532903,45581212,45566589,45585999,45532906,45581215,45600496,45561788,45537789,45542610,45547512,45542589,45581226,45552275,45576308,45556965,45571490,45566581,45600477,45542581,45605259,45561793,45556968,45600521,45532918,45590878,45537787,45600478,45566573,45576299,45585986,45600519,45532917,45605257,45576294,45595646,45585990,45576302,45561811,45561816,45552271,45552270,45532925,45537830,45605280,45542580,45532892,45605256,45537803,45576297,45595669,45561786,45566652,45542646,45537879,45755349,45595711,45755344,45542659,45595739,45547582,45566671,45581291,45566673,45542687,45590986,45542688,45590987,45571603,45561883,45561884,45542673,45557051,45561886,45566657,45561887,45586078,45542675,45576372,45542677,45537814,45576317,45586007,45595658,45571510,45556983,45566622,45755343,45566693,45547558,45576370,45542670,45590969,45600562,45755347,45605312,45600571,42616300,45557083,45605369,45571570,45561878,45537876,45591006,45595759,45552350,45581318,45586104,45542702,45755364,45571611,45600599,45576398,45571621,45561919,45542698,45537910,45586112,45586108,45605364,45566690,45605370,45537911,45561918,45561917,45552347,45566694,45605365,45581316,45571612,45581317,45605359,45581311,45547596,45595754,45586102,45566689,45571613,45542703,45532986,45552349,45532982,45532983,45571620,45557075,45586114,45586106,45576396,45566691,45561920,45571624,45532984,45595760,45600596,45561912,45605368,45552351,45552348,45605363,45571614,45576401,45586113,45542697,45571619,45586107,45591003,45542700,45605358,45605366,45605357,45537909,45547602,45552352,45600595,45586030,45547530,45561867,45542649,45755342,42619338,45595727,45547547,45557065,45561903,45590991,45590992,45590985,45590988,45600586,45561889,45552293,45532957,45581262,45755351,45561874,45552309,45537806,45561857,45581258,45595762,45600566,45586111,45576359,45595725,45581264,45537915,45755346,45595715,45600546,45571560,45552310,45595761,45590961,45542661,45581254,45586109,42616299,45532985,45571623,45537912,45600597,45586074,45561830,45571538,45542627,45576338,45595685,45557012,45557014,45590925,45557013,45590926,45552284,45571540,45561831,45547523,45595687,45595688,45537839,45532936,45576339,45561832,45586028,45537841,45547524,45532938,45552282,45755341,45547522,45571539,45595686,45600529,45590939,45586061,45755329,45755348,45755350,45532956,45542691,45542692,45595708,45542651,45755330,45605307,45581263,45557037)

) I
) C;
INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 15 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4112985,40488900,4111010,4307263,4200884,4178976,4111011,44501526,4116229,36561819,44500451,4131909,4116228,37110333,4315809,4112986)

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
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 12))
) C


-- End Procedure Occurrence Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
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

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 15))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,30,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, DATEADD(d,1,C.procedure_date) as END_DATE,
       C.procedure_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.procedure_date as sort_date
from 
(
  select po.* 
  FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN #Codesets codesets on ((po.procedure_concept_id = codesets.concept_id and codesets.codeset_id = 2))
) C


-- End Procedure Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,0,P.START_DATE) AND A.START_DATE <= P.OP_END_DATE
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 2 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_2
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
LEFT JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_source_concept_id = codesets.concept_id and codesets.codeset_id = 14))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,30,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_2) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;

TRUNCATE TABLE #Inclusion_2;
DROP TABLE #Inclusion_2;


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
{3 != 0}?{
  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),3)-1)
}
)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
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
