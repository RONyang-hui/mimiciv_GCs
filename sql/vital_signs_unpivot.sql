-- 步骤1：删除已存在的名为“vital_signs_unpivot”的物化视图（如果有）
-- 步骤2：创建名为“vital_signs_unpivot”的新物化视图
drop materialized view if exists vital_signs_unpivot_gcs;
create materialized view vital_signs_unpivot_gcs as
with summary as (
    select distinct stay_id, label
    -- 对每个icustay_id和label分区，根据charttime升序获取首个valuenum值
    , first_value(valuenum) over (partition by stay_id, label order by charttime) as fst_val
    -- 对每个icustay_id和label分区，根据valuenum升序获取首个值（即最小值）
    , first_value(valuenum) over (partition by stay_id, label order by valuenum) as min_val
     -- 对每个icustay_id和label分区，根据valuenum降序获取首个值（即最大值）
    , first_value(valuenum) over (partition by stay_id, label order by valuenum desc) as max_val
    from vital_signs_cohort_gcs
)

select stay_id
, max(case when label = 'heart_rate' then 1 else 0 end) as vs_heart_rate_flag
, max(case when label = 'heart_rate' then fst_val else null end) as vs_heart_rate_first
, max(case when label = 'heart_rate' then min_val else null end) as vs_heart_rate_min
, max(case when label = 'heart_rate' then max_val else null end) as vs_heart_rate_max
, max(case when label = 'cvp' then 1 else 0 end) as vs_cvp_flag
, max(case when label = 'cvp' then fst_val else null end) as vs_cvp_first
, max(case when label = 'cvp' then min_val else null end) as vs_cvp_min
, max(case when label = 'cvp' then max_val else null end) as vs_cvp_max
, max(case when label = 'map' then 1 else 0 end) as vs_map_flag
, max(case when label = 'map' then fst_val else null end) as vs_map_first
, max(case when label = 'map' then min_val else null end) as vs_map_min
, max(case when label = 'map' then max_val else null end) as vs_map_max
, max(case when label = 'temp' then 1 else 0 end) as vs_temp_flag
, max(case when label = 'temp' then fst_val else null end) as vs_temp_first
, max(case when label = 'temp' then min_val else null end) as vs_temp_min
, max(case when label = 'temp' then max_val else null end) as vs_temp_max
from summary
group by stay_id
