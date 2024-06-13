drop materialized view if exists merged_data_raw;
create materialized view merged_data_raw as
select distinct *
from mimiciv_hosp.basics_tyg
left join vital_signs_unpivot using (stay_id)
left join lab_unpivot_tyg using (hadm_id)
left join mimiciv_hosp.lab_unpivot_tyg_TGadd using (hadm_id)
left join first_day_lab using (stay_id)
left join icustay_detail using (stay_id)



drop materialized view if exists merged_data_raw_gcs;
create materialized view merged_data_raw_gcs as
select distinct *
from mimiciv_hosp.basics_gcs
left join vital_signs_unpivot_gcs using (stay_id)
natural left join lab_unpivot_gcs 
natural left join mimiciv_derived.icustay_detail