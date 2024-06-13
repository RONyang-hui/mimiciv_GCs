with ab_tbl AS(
select
      abx.subject_id, abx.hadm_id, abx.stay_id
    , abx.antibiotic
    , abx.starttime AS antibiotic_time
    -- date is used to match microbiology cultures with only date available
    , DATE_TRUNC('DAY', abx.starttime) AS antibiotic_date
    , abx.stoptime AS antibiotic_stoptime
    -- create a unique identifier for each patient antibiotic
    , ROW_NUMBER() OVER
    (
      PARTITION BY subject_id
      ORDER BY starttime, stoptime, antibiotic
    ) AS ab_id
  from mimiciv_derived.antibiotic abx
),
	  -- 在microbiologyevents表里面同一个micro_specimen_id有重复记录
  -- 先通过下面代码合并为一个记录
me AS(
select micro_specimen_id
  , MAX(subject_id) AS subject_id
  , MAX(hadm_id) AS hadm_id
  , CAST(MAX(chartdate) AS DATE) AS chartdate
  , MAX(charttime) AS charttime
  , MAX(spec_type_desc) AS spec_type_desc
  -- 如果有病原学(organism)结果就为阳性
  , max(case when org_name is not null and org_name != '' then 1 else 0 end) as PositiveCulture
from mimiciv_hosp.microbiologyevents
group by micro_specimen_id
),

me_then_ab AS(
select
    ab_tbl.subject_id
    , ab_tbl.hadm_id
    , ab_tbl.stay_id
    , ab_tbl.ab_id
    , me72.micro_specimen_id
    , coalesce(me72.charttime, DATETIME(me72.chartdate)) as last72_charttime
    , me72.positiveculture as last72_positiveculture
    , me72.spec_type_desc as last72_specimen
    , ROW_NUMBER() OVER
    (
    -- 同一个病人一次抗生素医嘱期间多次的病原学检查排序为micro_seq
      PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
      ORDER BY me72.chartdate, me72.charttime NULLS LAST
    ) AS micro_seq
  from ab_tbl
  LEFT JOIN me me72
    on ab_tbl.subject_id = me72.subject_id
    and
    (
      (
    -- 送检后三天内抗生素医嘱
      -- 有charttime就用charttime
          me72.charttime is not null
      and ab_tbl.antibiotic_time > me72.charttime
      and ab_tbl.antibiotic_time <= DATETIME_ADD(me72.charttime, INTERVAL '72' HOUR) 
      )
      OR
      (
      -- 没有charttime就用chartdate
          me72.charttime is null
      and antibiotic_date >= me72.chartdate
      and antibiotic_date <= DATE_ADD(me72.chartdate, INTERVAL '3' DAY)
      )
    )
),

ab_then_me AS(
select
      ab_tbl.subject_id
    , ab_tbl.hadm_id
    , ab_tbl.stay_id
    , ab_tbl.ab_id
    
    , me24.micro_specimen_id
    , COALESCE(me24.charttime, DATETIME(me24.chartdate)) as next24_charttime
    , me24.positiveculture as next24_positiveculture
    , me24.spec_type_desc as next24_specimen
    , ROW_NUMBER() OVER
    (
      PARTITION BY ab_tbl.subject_id, ab_tbl.ab_id
      ORDER BY me24.chartdate, me24.charttime NULLS LAST
    ) AS micro_seq
  from ab_tbl
  -- culture in subsequent 24 hours
  LEFT JOIN me me24
    on ab_tbl.subject_id = me24.subject_id
    and
    (
      (
          me24.charttime is not null
      and ab_tbl.antibiotic_time >= DATETIME_SUB(me24.charttime, INTERVAL '24' HOUR)  
      and ab_tbl.antibiotic_time < me24.charttime
      )
      OR
      (
          me24.charttime is null
      and ab_tbl.antibiotic_date >= DATE_SUB(me24.chartdate, INTERVAL '1' DAY)
      and ab_tbl.antibiotic_date <= me24.chartdate
      )
    )
),


SELECT
ab_tbl.subject_id
, ab_tbl.stay_id
, ab_tbl.hadm_id
, ab_tbl.ab_id
, ab_tbl.antibiotic
, ab_tbl.antibiotic_time
, CASE
  WHEN last72_specimen IS NULL AND next24_specimen IS NULL
    THEN 0
  ELSE 1 
  END AS suspected_infection
, CASE
  WHEN last72_specimen IS NULL AND next24_specimen IS NULL
    THEN NULL
  ELSE COALESCE(last72_charttime, antibiotic_time)
  END AS suspected_infection_time
  -- coalesce就是从参数选第一个不为空的返回
, COALESCE(last72_charttime, next24_charttime) AS culture_time
, COALESCE(last72_specimen, next24_specimen) AS specimen
, COALESCE(last72_positiveculture, next24_positiveculture) AS positive_culture
FROM ab_tbl
LEFT JOIN ab_then_me ab2me
    ON ab_tbl.subject_id = ab2me.subject_id
    AND ab_tbl.ab_id = ab2me.ab_id
    AND ab2me.micro_seq = 1
LEFT JOIN me_then_ab me2ab
    ON ab_tbl.subject_id = me2ab.subject_id
    AND ab_tbl.ab_id = me2ab.ab_id
    AND me2ab.micro_seq = 1