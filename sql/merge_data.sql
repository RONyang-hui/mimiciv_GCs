drop materialized view if exists merged_data_gcs;
create materialized view merged_data_gcs as
select stay_id
, hadm_id
, subject_id
, gender
, admission_age AS age
, sapsii
, icu_intime
, icu_outtime
, weight 
, sofa_24hours
, mort_28_day
, survival_days
, gcs_include
, vs_heart_rate_first
, vs_heart_rate_min
, vs_heart_rate_max
, vs_cvp_first
, vs_cvp_min
, vs_cvp_max
, vs_map_first
, vs_map_min
, vs_map_max
, vs_temp_first
, vs_temp_min
, vs_temp_max
, lab_hemoglobin_first
, lab_hemoglobin_min
, lab_hemoglobin_max
, lab_hemoglobin_abnormal
, lab_platelet_first
, lab_platelet_min
, lab_platelet_max
, lab_platelet_abnormal
, lab_creatinine_kinase_first
, lab_creatinine_kinase_min
, lab_creatinine_kinase_max
, lab_creatinine_kinase_abnormal
, lab_wbc_first
, lab_wbc_min
, lab_wbc_max
, lab_wbc_abnormal
, lab_ph_first
, lab_ph_min
, lab_ph_max
, lab_ph_abnormal
, lab_chloride_first
, lab_chloride_min
, lab_chloride_max
, lab_chloride_abnormal
, lab_sodium_first
, lab_sodium_min
, lab_sodium_max
, lab_sodium_abnormal
, lab_bun_first
, lab_bun_min
, lab_bun_max
, lab_bun_abnormal
, lab_bicarbonate_first
, lab_bicarbonate_min
, lab_bicarbonate_max
, lab_bicarbonate_abnormal
, lab_pco2_first
, lab_pco2_min
, lab_pco2_max
, lab_pco2_abnormal
, lab_creatinine_first
, lab_creatinine_min
, lab_creatinine_max
, lab_creatinine_abnormal
, lab_potassium_first
, lab_potassium_min
, lab_potassium_max
, lab_potassium_abnormal
, lab_troponin_first
, lab_troponin_min
, lab_troponin_max
, lab_troponin_abnormal
, lab_po2_first
, lab_po2_min
, lab_po2_max
, lab_po2_abnormal
, lab_lactate_first
, lab_lactate_min
, lab_lactate_max
, lab_lactate_abnormal
, cam_state
, pre_dose_val_rx
, los_drug
, (pre_dose_val_rx * los_drug) AS cumulative_dose
, gcs_include AS gcs
, CASE WHEN dose_val_rx IS NULL AND pre_dose_val_rx IS NOT NULL THEN pre_dose_val_rx ELSE dose_val_rx END AS dose_val_rx_average
, CASE WHEN after_los_drug IS NULL AND los_drug IS NOT NULL THEN los_drug ELSE after_los_drug END AS los_drug_average
, case when dod is null then 0 else 1 end as event_state
, case when vs_heart_rate_flag is null then 0 else vs_heart_rate_flag end as vs_heart_rate_flag
, case when vs_cvp_flag is null then 0 else vs_cvp_flag end as vs_cvp_flag
, case when vs_map_flag is null then 0 else vs_map_flag end as vs_map_flag
, case when vs_temp_flag is null then 0 else vs_temp_flag end as vs_temp_flag
, case when lab_hemoglobin_flag is null then 0 else lab_hemoglobin_flag end as lab_hemoglobin_flag
, case when lab_platelet_flag is null then 0 else lab_platelet_flag end as lab_platelet_flag
, case when lab_creatinine_kinase_flag is null then 0 else lab_creatinine_kinase_flag end as lab_creatinine_kinase_flag
, case when lab_wbc_flag is null then 0 else lab_wbc_flag end as lab_wbc_flag
, case when lab_ph_flag is null then 0 else lab_ph_flag end as lab_ph_flag
, case when lab_chloride_flag is null then 0 else lab_chloride_flag end as lab_chloride_flag
, case when lab_sodium_flag is null then 0 else lab_sodium_flag end as lab_sodium_flag
, case when lab_bun_flag is null then 0 else lab_bun_flag end as lab_bun_flag
, case when lab_bicarbonate_flag is null then 0 else lab_bicarbonate_flag end as lab_bicarbonate_flag
, case when lab_bnp_flag is null then 0 else lab_bnp_flag end as lab_bnp_flag
, case when lab_pco2_flag is null then 0 else lab_pco2_flag end as lab_pco2_flag
, case when lab_creatinine_flag is null then 0 else lab_creatinine_flag end as lab_creatinine_flag
, case when lab_potassium_flag is null then 0 else lab_potassium_flag end as lab_potassium_flag
, case when lab_troponin_flag is null then 0 else lab_troponin_flag end as lab_troponin_flag
, case when lab_po2_flag is null then 0 else lab_po2_flag end as lab_po2_flag
, case when lab_lactate_flag is null then 0 else lab_lactate_flag end as lab_lactate_flag

from merged_data_raw_gcs;

WHERE lab_TG_first is not null;
nsaids_include

SELECT count(*) lab_TG_first
FROM merged_data_tyg2

SELECT count(*) TyG
FROM merged_data_tyg

SELECT count(*) mort_28_day
FROM merged_data_tyg2
WHERE mort_28_day = 0


