CREATE TABLE GCs_icu_50920 AS 
SELECT * 
FROM mimiciv_derived.icustay_detail 
WHERE hospstay_seq = 1 
AND icustay_seq = 1;

-- 2. 纳入ICU停留时间≥1天的患者
CREATE TABLE GCs_icu_40298 AS 
SELECT * 
FROM GCs_icu_50920 
WHERE los_icu >= 1;
--筛选出23726条记录
--排除50920-10622 = 40298条记录


CREATE TABLE GCs_icu_50920 AS 
SELECT * 
FROM mimiciv_derived.icustay_detail 
WHERE hospstay_seq = 1 
AND icustay_seq = 1;

-- 2. 纳入ICU停留时间≥1天的患者
CREATE TABLE GCs_icu_40298 AS 
SELECT * 
FROM GCs_icu_50920 
WHERE los_icu >= 1;
-- 筛选出23726条记录
-- 排除50920-10622 = 40298条记录
DROP TABLE IF EXISTS GCs_icu_2879_drug1;
CREATE TABLE GCs_icu_2879_drug1 AS
-- 提取ICU后使用他汀类药物的ICU患者记录
WITH after_icu AS (
    SELECT DISTINCT ON (ie.subject_id, ie.hadm_id, ie.stay_id)
        ie.subject_id, ie.hadm_id, ie.stay_id, ie.intime, st.starttime, st.drug, st.los_drug AS after_los_drug , st.dose_val_rx, st.dose_unit_rx,
        1 AS after_icu_GCs
    FROM mimiciv_icu.icustays ie
    LEFT JOIN GC_prescriptions1 st USING (hadm_id)
    WHERE st.starttime > ie.intime
    ORDER BY ie.subject_id, ie.hadm_id, ie.stay_id
),
pre_icu AS (
    SELECT DISTINCT ON (ie.subject_id, ie.hadm_id, ie.stay_id)
        ie.subject_id, ie.hadm_id, ie.stay_id, ie.intime, st.starttime, st.drug, st.los_drug, st.dose_val_rx, st.dose_unit_rx,
        1 AS pre_icu_GCs
    FROM mimiciv_icu.icustays ie
    LEFT JOIN GC_prescriptions1 st USING (hadm_id)
    WHERE st.starttime < ie.intime
    ORDER BY ie.subject_id, ie.hadm_id, ie.stay_id
)
SELECT GCs.*, after_icu.after_icu_GCs, pre_icu.pre_icu_GCs, pre_icu.los_drug, after_icu.after_los_drug,
    after_icu.dose_val_rx, after_icu.dose_unit_rx, pre_icu.dose_val_rx AS pre_dose_val_rx, pre_icu.dose_unit_rx AS pre_dose_unit_rx,
    CASE WHEN after_icu_GCs IS NULL AND pre_icu_GCs IS NULL THEN 1 ELSE 0 END AS GCs_exclude,
    CASE WHEN after_icu_GCs = 1 OR pre_icu_GCs = 1 THEN 1 ELSE 0 END AS GCs_include_combine,
		CASE WHEN pre_icu_GCs = 1 THEN 1 ELSE 0 END AS GCs_include
FROM GCs_icu_40298 GCs
LEFT JOIN after_icu USING (stay_id)
LEFT JOIN pre_icu USING (stay_id);


SELECT SUM(GCs_include) AS GCs_include_count
FROM GCs_icu_2879_drug1;


SELECT SUM(GCs_include_combine) AS GCs_include_combine_count
FROM GCs_icu_2879_drug1;


SELECT SUM(nsaids_include_combine) AS nsaids_include_combine_count
FROM nsaids_icu_2879_drug1;



DROP TABLE IF EXISTS cohort_GCs CASCADE;
CREATE TABLE cohort_GCs  AS
    SELECT*
    FROM immune_group
    LEFT JOIN GCs_icu_2879_drug1
		USING(subject_id)
		WHERE GCs_include IS NOT NULL


		
SELECT SUM(GCs _include) AS GCs _include_count
FROM cohort_GCs ;

SELECT SUM(GCs _include_combine) AS GCs _include_combine_count
FROM cohort_GCs ;


SELECT COUNT(*) AS subject_id_count
FROM cohort_GCs 



-- 选择最大的MAX(cam.charttime)，且cam.charttime > CO.icu_intime;
DROP TABLE IF EXISTS cohort_nsaids;
CREATE TABLE cohort_nsaids AS 
SELECT subject_id_alias, stay_id, subject_id, hadm_id, gender, dod, admittime, dischtime, los_hospital, admission_age, race, hospital_expire_flag, hospstay_seq, first_hosp_stay, icu_intime, icu_outtime, los_icu, icustay_seq, first_icu_stay, after_icu_statin, pre_icu_statin, stain_exclude, stain_include, max_charttime, valuenum
FROM (
    SELECT CO.subject_id AS subject_id_alias, CO.*, 
           MAX(cam.charttime) OVER (PARTITION BY CO.subject_id) AS max_charttime,
           cam.valuenum,
           ROW_NUMBER() OVER (PARTITION BY CO.subject_id ORDER BY cam.charttime DESC) AS rn
    FROM COPD_6426_flag AS CO
    LEFT JOIN mimiciv_icu.cam_icd AS cam USING (subject_id)
    WHERE cam.charttime > CO.icu_intime
) AS subquery
WHERE rn = 1;
		
		
