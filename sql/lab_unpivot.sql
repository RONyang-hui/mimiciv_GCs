-- 步骤1：在创建新的物化视图之前，首先删除已存在的名为“lab_unpivot”的物化视图。
drop materialized view if exists lab_unpivot_gcs;
create materialized view lab_unpivot_gcs as
    -- 从"lab_tests"物化视图中选择不重复的hadm_id和label，并计算每个组合的第一个值、最小值、最大值和异常情况
-- 使用WITH子句创建一个名为"lab_summary"的子查询
 -- 从"lab_tests"物化视图中选择不重复的hadm_id和label，并进行以下计算：
with lab_summary as (
    select distinct hadm_id, label
    , first_value(valuenum) over (partition by hadm_id, label order by charttime) as fst_val  -- 对于每个组合的hadm_id和label，按照charttime的顺序，获取第一个valuenum的值作为fst_val
    , first_value(valuenum) over (partition by hadm_id, label order by valuenum) as min_val  -- 对于每个组合的hadm_id和label，按照valuenum的顺序，获取最小valuenum的值作为min_val
    , first_value(valuenum) over (partition by hadm_id, label order by valuenum desc) as max_val  -- 对于每个组合的hadm_id和label，按照valuenum的降序顺序，获取最大valuenum的值作为max_val
    , first_value(abnormal) over (partition by hadm_id, label order by abnormal desc) as abnormal  -- 对于每个组合的hadm_id和label，按照abnormal的降序顺序，获取第一个abnormal的值作为abnormal
    from lab_tests_gcs
)
-- 步骤3：基于中间表 `lab_summary` 进行更细致的数据提取与整合
-- 是否存在血红蛋白记录的标志、血红蛋白的第一个值、最小值、最大值以及异常情况。
select hadm_id
, max(case when label = 'hemoglobin' then 1 else 0 end) as lab_hemoglobin_flag
, max(case when label = 'hemoglobin' then fst_val else null end) as lab_hemoglobin_first
, max(case when label = 'hemoglobin' then min_val else null end) as lab_hemoglobin_min
, max(case when label = 'hemoglobin' then max_val else null end) as lab_hemoglobin_max
, max(case when label = 'hemoglobin' then abnormal else null end) as lab_hemoglobin_abnormal
-- 是否存在platelet记录的标志、platelet的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'platelet' then 1 else 0 end) as lab_platelet_flag
, max(case when label = 'platelet' then fst_val else null end) as lab_platelet_first
, max(case when label = 'platelet' then min_val else null end) as lab_platelet_min
, max(case when label = 'platelet' then max_val else null end) as lab_platelet_max
, max(case when label = 'platelet' then abnormal else null end) as lab_platelet_abnormal
-- 是否存在creatinine_kinaset记录的标志、creatinine_kinaset的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'creatinine_kinase' then 1 else 0 end) as lab_creatinine_kinase_flag
, max(case when label = 'creatinine_kinase' then fst_val else null end) as lab_creatinine_kinase_first
, max(case when label = 'creatinine_kinase' then min_val else null end) as lab_creatinine_kinase_min
, max(case when label = 'creatinine_kinase' then max_val else null end) as lab_creatinine_kinase_max
, max(case when label = 'creatinine_kinase' then abnormal else null end) as lab_creatinine_kinase_abnormal
-- 是否存在wbc记录的标志、wbc的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'wbc' then 1 else 0 end) as lab_wbc_flag
, max(case when label = 'wbc' then fst_val else null end) as lab_wbc_first
, max(case when label = 'wbc' then min_val else null end) as lab_wbc_min
, max(case when label = 'wbc' then max_val else null end) as lab_wbc_max
, max(case when label = 'wbc' then abnormal else null end) as lab_wbc_abnormal
-- 是否存在ph记录的标志、ph的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'ph' then 1 else 0 end) as lab_ph_flag
, max(case when label = 'ph' then fst_val else null end) as lab_ph_first
, max(case when label = 'ph' then min_val else null end) as lab_ph_min
, max(case when label = 'ph' then max_val else null end) as lab_ph_max
, max(case when label = 'ph' then abnormal else null end) as lab_ph_abnormal
-- 是否存在chloride记录的标志、chloride的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'chloride' then 1 else 0 end) as lab_chloride_flag
, max(case when label = 'chloride' then fst_val else null end) as lab_chloride_first
, max(case when label = 'chloride' then min_val else null end) as lab_chloride_min
, max(case when label = 'chloride' then max_val else null end) as lab_chloride_max
, max(case when label = 'chloride' then abnormal else null end) as lab_chloride_abnormal
-- 是否存在sodium记录的标志、sodium的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'sodium' then 1 else 0 end) as lab_sodium_flag
, max(case when label = 'sodium' then fst_val else null end) as lab_sodium_first
, max(case when label = 'sodium' then min_val else null end) as lab_sodium_min
, max(case when label = 'sodium' then max_val else null end) as lab_sodium_max
, max(case when label = 'sodium' then abnormal else null end) as lab_sodium_abnormal
-- 是否存在bun记录的标志、bun的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'bun' then 1 else 0 end) as lab_bun_flag
, max(case when label = 'bun' then fst_val else null end) as lab_bun_first
, max(case when label = 'bun' then min_val else null end) as lab_bun_min
, max(case when label = 'bun' then max_val else null end) as lab_bun_max
, max(case when label = 'bun' then abnormal else null end) as lab_bun_abnormal
-- 是否存在bicarbonate记录的标志、bicarbonate的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'bicarbonate' then 1 else 0 end) as lab_bicarbonate_flag
, max(case when label = 'bicarbonate' then fst_val else null end) as lab_bicarbonate_first
, max(case when label = 'bicarbonate' then min_val else null end) as lab_bicarbonate_min
, max(case when label = 'bicarbonate' then max_val else null end) as lab_bicarbonate_max
, max(case when label = 'bicarbonate' then abnormal else null end) as lab_bicarbonate_abnormal
-- 是否存在bnp记录的标志、bnp的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'bnp' then 1 else 0 end) as lab_bnp_flag
, max(case when label = 'bnp' then fst_val else null end) as lab_bnp_first
, max(case when label = 'bnp' then min_val else null end) as lab_bnp_min
, max(case when label = 'bnp' then max_val else null end) as lab_bnp_max
, max(case when label = 'bnp' then abnormal else null end) as lab_bnp_abnormal
-- 是否存在pco2记录的标志、pco2的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'pco2' then 1 else 0 end) as lab_pco2_flag
, max(case when label = 'pco2' then fst_val else null end) as lab_pco2_first
, max(case when label = 'pco2' then min_val else null end) as lab_pco2_min
, max(case when label = 'pco2' then max_val else null end) as lab_pco2_max
, max(case when label = 'pco2' then abnormal else null end) as lab_pco2_abnormal
-- 是否存在creatinine记录的标志、creatinine的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'creatinine' then 1 else 0 end) as lab_creatinine_flag
, max(case when label = 'creatinine' then fst_val else null end) as lab_creatinine_first
, max(case when label = 'creatinine' then min_val else null end) as lab_creatinine_min
, max(case when label = 'creatinine' then max_val else null end) as lab_creatinine_max
, max(case when label = 'creatinine' then abnormal else null end) as lab_creatinine_abnormal
-- 是否存在potassium记录的标志、potassium的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'potassium' then 1 else 0 end) as lab_potassium_flag
, max(case when label = 'potassium' then fst_val else null end) as lab_potassium_first
, max(case when label = 'potassium' then min_val else null end) as lab_potassium_min
, max(case when label = 'potassium' then max_val else null end) as lab_potassium_max
, max(case when label = 'potassium' then abnormal else null end) as lab_potassium_abnormal
-- 是否存在troponin记录的标志、troponin的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'troponin' then 1 else 0 end) as lab_troponin_flag
, max(case when label = 'troponin' then fst_val else null end) as lab_troponin_first
, max(case when label = 'troponin' then min_val else null end) as lab_troponin_min
, max(case when label = 'troponin' then max_val else null end) as lab_troponin_max
, max(case when label = 'troponin' then abnormal else null end) as lab_troponin_abnormal
-- 是否存在po2记录的标志、po2的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'po2' then 1 else 0 end) as lab_po2_flag
, max(case when label = 'po2' then fst_val else null end) as lab_po2_first
, max(case when label = 'po2' then min_val else null end) as lab_po2_min
, max(case when label = 'po2' then max_val else null end) as lab_po2_max
, max(case when label = 'po2' then abnormal else null end) as lab_po2_abnormal
-- 是否存在lactate记录的标志、lactate的第一个值、最小值、最大值以及异常情况。
, max(case when label = 'lactate' then 1 else 0 end) as lab_lactate_flag
, max(case when label = 'lactate' then fst_val else null end) as lab_lactate_first
, max(case when label = 'lactate' then min_val else null end) as lab_lactate_min
, max(case when label = 'lactate' then max_val else null end) as lab_lactate_max
, max(case when label = 'lactate' then abnormal else null end) as lab_lactate_abnormal



from lab_summary
group by hadm_id
-- 最后，按照住院记录的唯一标识 hadm_id 进行分组

-- 最终的 lab_unpivot 视图包含了每种实验室指标是否存在记录的标志（flag）、首个值、最小值、最大值以及异常情况的标记，这样用户可以通过查询 lab_unpivot 视图快速获取任何一项实验室指标的所有关键统计数据，而无需分别对多个指标执行查询操作。

-- 该视图基于 lab_tests 物化视图进行更深度的数据加工和整理，它主要是为了提供一种扁平化的数据结构，将所有实验室指标（如血红蛋白、血小板、肌酐、BUN等）整合在同一组列中，便于后续统计分析和报告。
-- lab_unpivot 通过 WITH 子句首先生成了一个名为 lab_summary 的中间表，该表对 lab_tests 中的每个 hadm_id 和 label（实验室指标）组合计算了首个值（fst_val）、最小值（min_val）、最大值（max_val）以及异常情况（abnormal）。