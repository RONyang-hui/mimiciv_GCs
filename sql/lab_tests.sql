-- 删除已存在的物化视图"lab_tests"
-- 该视图首先从原始的 labevents 表中提取有关实验室检查项目的结果数据，将不同的 itemid 映射到具体的实验室指标（例如白细胞计数WBC、血红蛋白HGB等），同时保留对应的 hadm_id（住院ID）、charttime（记录时间）和 valuenum（数值型结果）。
-- 这个视图还通过关联 basics 表来考虑性别因素，对各个指标的正常范围进行了定义，并依据这个范围给每个指标赋予了异常状态标识（abnormal）
drop materialized view if exists lab_tests;
-- 创建物化视图"lab_tests"
create materialized view lab_tests as
with lab_tests as (
    select hadm_id, charttime, value,
           case when itemid in (51300,51301) then 'wbc'
                when itemid in (50811,51222) then 'hemoglobin'
                when itemid in (51265) then 'platelet'
                when itemid in (50824,50983) then 'sodium'
                when itemid in (50822,50971) then 'potassium'
                when itemid in (50882) then 'bicarbonate'
                when itemid in (50806,50902) then 'chloride'
                when itemid in (51006) then 'bun'
                when itemid in (50813) then 'lactate'
                when itemid in (50912) then 'creatinine'
                when itemid in (50820) then 'ph'
                when itemid in (50821) then 'po2'
                when itemid in (50818) then 'pco2'
                when itemid in (50963) then 'bnp'
                when itemid in (51002,51003) then 'troponin'
                when itemid in (50910,50911) then 'creatinine_kinase'
                when itemid in (51000, 51000,51060,51803,51824,51950) then 'Triglycerides'
                else null end as label,
           valuenum
    from mimiciv_hosp.labevents lab
)

drop materialized view if exists lab_tests_gcs;
-- 创建物化视图"lab_tests"
create materialized view lab_tests_gcs as
with lab_tests_gcs as (
    -- 从"basics"表和"lab_tests"临时表中选择相关列，并为异常结果赋值
    -- 根据标签(label)和值(valuenum)判断是否异常，赋值0或1
    select hadm_id, lab.charttime, lab.label, lab.valuenum,
           case when label = 'wbc' and lab.valuenum between 4.5 and 10 then 0 -- 白细胞计数正常范围为4.5到10
                when label = 'hgb' and co.gender = 'M' and lab.valuenum between 13.8 and 17.2 then 0
                -- 男性血红蛋白正常范围为13.8到17.2
                when label = 'hgb' and co.gender = 'F' and lab.valuenum between 12.1 and 15.1 then 0
                -- 女性血红蛋白正常范围为12.1到15.1
                when label = 'platelet' and lab.valuenum between 150 and 400 then 0
                -- 血小板计数正常范围为150到400
                when label = 'sodium' and lab.valuenum between 135 and 145 then 0
                -- 钠正常范围为135到145
                when label = 'potassium' and lab.valuenum between 3.7 and 5.2 then 0
                -- 钾正常范围为3.7到5.2
                when label = 'bicarbonate' and lab.valuenum between 22 and 28 then 0
                -- 碳酸氢根离子正常范围为22到28
                when label = 'chloride' and lab.valuenum between 96 and 106 then 0
                -- 氯正常范围为96到106
                when label = 'bun' and lab.valuenum between 6 and 20 then 0
                -- 尿素氮正常范围为6到20
                when label = 'lactate' and lab.valuenum between 0.5 and 2.2 then 0
                -- 乳酸正常范围为0.5到2.2
                when label = 'creatinine' and co.gender = 'M' and lab.valuenum <= 1.3 then 0
                -- 男性肌酐正常范围为小于等于1.3
                when label = 'creatinine' and co.gender = 'F' and lab.valuenum <= 1.1 then 0
                -- 女性肌酐正常范围为小于等于1.1
                when label = 'ph' and lab.valuenum between 7.38 and 7.42 then 0
                -- pH值正常范围为7.38到7.42
                when label = 'po2' and lab.valuenum between 75 and 100 then 0
                -- 氧分压正常范围为75到100
                when label = 'pco2' and lab.valuenum between 35 and 45 then 0
                -- 二氧化碳分压正常范围为35到45
                when label = 'bnp' and lab.valuenum <= 100 then 0
                -- 脑利钠肽正常范围为小于等于100
                when label = 'troponin' and lab.valuenum <= 0.1 then 0
                -- 肌钙蛋白正常范围为小于等于0.1
                when label = 'creatinine_kinase' and lab.valuenum <= 120 then 0
                -- 肌酸激酶正常范围为小于等于120
                when label = 'Triglycerides' and lab.valuenum BETWEEN 0 AND 150 THEN 0
                else 1 end as abnormal

                
    from basics_gcs co
    left join lab_tests lab using (hadm_id)
    where charttime between icu_intime and icu_intime + interval '1 day'
          -- 选择检测时间在入院后的第一天
          and charttime between icu_intime and icu_outtime
          -- 选择检测时间在入院期间
          and label is not null
          -- 标签(label)不为空
          and lab.valuenum is not null
          -- 值(valuenum)不为空
          and lab.valuenum > 0
          -- 值(valuenum)大于0
)

select * from lab_tests_gcs;












