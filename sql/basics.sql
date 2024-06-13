-- 删除名为'basics_hcq'的物化视图，如果存在的话
drop materialized view if exists basics_gcs cascade;

-- 创建名为'basics_hcq'的物化视图
create materialized view basics_imm as
with mort as (
    -- 从'mimiciii.admissions'和'mimiciii.patients'表中检索每位患者的死亡时间
    select co.hadm_id,
           coalesce(adm.deathtime, pat.dod, null) as deathtime
    from mimiciv_hosp.cohort_imm co
    left join (select hadm_id, deathtime from mimiciv_hosp.admissions) adm using (hadm_id)
     natural left join mimiciv_hosp.patients pat
),
mort_28 as (
    -- 确定患者是否在icu入院后的28天内死亡
    select hadm_id,
           case when deathtime <= (co.intime + interval '28' day) then 1 else 0 end as mort_28_day
    from mimiciv_hosp.cohort_imm co
    natural left join mort
),
survival_days as (
    -- 统计生存时间
    select hadm_id,
           extract(epoch from (coalesce(deathtime, co.outtime) - co.intime))/86400 as survival_days
    from mimiciv_hosp.cohort_imm co
    natural left join mort
)
,
cam_state as (
    -- 统计生存时间
    select hadm_id,
    , first_value(valuenum) over (partition by hadm_id, label order by valuenum desc) as cam_state
    from mimiciv_icu.cam_icd ca
    natural left join mort
)
,
crp as (
    -- 统计生存时间
    select hadm_id,
    , first_value(crp) over (partition by hadm_id, charttime order by crp desc) as crp
    from mimiciv_derived.inflammation inf
    natural left join mort
)
,
basics_gcs as (
    -- 将所有计算指标与原始患者信息结合，创建综合数据集
    select distinct on (stay_id) co.*, w.weight, sa.sapsii, so.sofa_24hours, mo.mort_28_day, survival_days.survival_days
    from mimiciv_hosp.cohort_imm co
    natural left join (select subject_id, gender from mimiciv_hosp.patients) g
    natural left join (select stay_id, weight from mimiciv_derived.first_day_weight) w
    natural left join (select stay_id, sapsii from mimiciv_derived.sapsii) sa
    natural left join (select stay_id, sofa_24hours from mimiciv_derived.sofa) so
    natural left join mort 
    natural left join mort_28 mo
    natural left join survival_days
    natural left join cam_icd ca
    natural left join inflammation inf
)
SELECT DISTINCT * FROM basics_gcs;



-- 删除名为'basics_hcq'的物化视图，如果存在的话
DROP MATERIALIZED VIEW IF EXISTS basics_gcs CASCADE;

-- 创建名为'basics_hcq'的物化视图
CREATE MATERIALIZED VIEW basics_gcs AS
WITH mort AS (
    -- 从'mimiciii.admissions'和'mimiciii.patients'表中检索每位患者的死亡时间
    SELECT co.hadm_id,
           COALESCE(adm.deathtime, pat.dod, NULL) AS deathtime
    FROM mimiciv_hosp.cohort_gcs co
    LEFT JOIN (SELECT hadm_id, deathtime FROM mimiciv_hosp.admissions) adm USING (hadm_id)
    NATURAL LEFT JOIN mimiciv_hosp.patients pat
),
mort_28 AS (
    -- 确定患者是否在icu入院后的28天内死亡
    SELECT hadm_id,
           CASE WHEN deathtime <= (co.icu_intime + INTERVAL '28' DAY) THEN 1 ELSE 0 END AS mort_28_day
    FROM mimiciv_hosp.cohort_gcs co
    NATURAL LEFT JOIN mort
),
survival_days AS (
    -- 统计生存时间
    SELECT hadm_id,
           EXTRACT(EPOCH FROM (COALESCE(deathtime, co.icu_outtime) - co.icu_intime)) / 86400 AS survival_days
    FROM mimiciv_hosp.cohort_gcs co
    NATURAL LEFT JOIN mort
),

cam_state as (
    -- 统计生存时间
    select DISTINCT hadm_id,
    first_value(valuenum) over (partition by hadm_id, charttime order by valuenum desc) as cam_state
    from mimiciv_icu.cam_icd ca
    natural left join mort
)
,
infection as (
    -- 统计生存时间
SELECT DISTINCT hadm_id, 
    first_value(suspected_infection) OVER (PARTITION BY hadm_id, suspected_infection_time ORDER BY suspected_infection) AS infection
FROM mimiciv_derived.suspicion_of_infection inf
NATURAL LEFT JOIN mimiciv_derived.icustay_detail
)
,

basics_gcs AS (
    -- 将所有计算指标与原始患者信息结合，创建综合数据集
    SELECT DISTINCT ON (co.stay_id)
           co.*,
           w.weight,
           sa.sapsii,
           so.sofa_24hours,
           mo.mort_28_day,
           sd.survival_days,
					 cam_state ca,
					 inf.suspected_infection 
    FROM mimiciv_hosp.cohort_gcs co
    NATURAL LEFT JOIN (SELECT subject_id, gender FROM mimiciv_hosp.patients) g
    NATURAL LEFT JOIN (SELECT stay_id, weight FROM mimiciv_derived.first_day_weight) w
    NATURAL LEFT JOIN (SELECT stay_id, sapsii FROM mimiciv_derived.sapsii) sa
    NATURAL LEFT JOIN (SELECT stay_id, sofa_24hours FROM mimiciv_derived.sofa) so
    NATURAL LEFT JOIN mort
    NATURAL LEFT JOIN mort_28 mo
    NATURAL LEFT JOIN survival_days sd
    natural left join cam_state ca
		natural left join mimiciv_derived.infection inf
)
SELECT DISTINCT * FROM basics_gcs;