
-- count表的制作
CREATE TABLE mimiciv_derived.d_icd_diagnoses_count AS
SELECT icd_code, icd_version, long_title, COUNT(icd_code) AS code_count
FROM mimiciv_hosp.d_icd_diagnoses
GROUP BY icd_code, icd_version, long_title;


-- 获取icd_code编码可以从这两张表中获取diagnoses_icd,d_icd_diagnoses
-- 但是无法判断具体是哪一个icd_code，所以可以从整理的汇总表中获取


-- 从汇总表中获取所有疾病的诊断总数
SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC
LIMIT 10;

1. 确诊为sepsis的患者 (n = 32971) √
/*
    This query selects patients from the mimiciv_derived.sepsis3 table
    where sepsis3 = t. It groups the patients by stay_id and sofa score.
*/
2. 至少接受血管加压治疗的患者 (n = 4397)




3. 年龄大于18且小于100的患者 (n = 4112)


01.sepsis_patients
--确诊为sepsis的患者
--查询18-60岁之间的患者
--查询患者的首次入ICU记录
CREATE VIEW sepsis_patients AS
SELECT *
FROM mimiciv_derived.sepsis3 AS s
JOIN mimiciv_derived.icustay_detail AS icu USING (subject_id, stay_id)
WHERE icu.admission_age >= 18 AND icu.admission_age <= 80
    AND icu.first_icu_stay = TRUE;

在此基础上，将这个查询的视图与mimiciv_derived.vasopressin表格进行对比（using stay_id），
    新建视图（VIEW）sepsis_patients_vaso表格，并新添加一列用来存储vasopressin的使用情况，如果使用过填1，否则为0



02.sepsis_patients
---在此基础上，将这个查询的表格与mimiciv_derived.vasopressin表格进行对比（using stay_id），
---在sepsis_patients表格中新添加一列用来存储vasopressin的使用情况，如果使用过填1，否则为0
CREATE VIEW sepsis_patients_vaso AS
SELECT DISTINCT ON (sp.subject_id) sp.*, CASE 
    WHEN v.stay_id IS NOT NULL THEN 1 
    ELSE 0 
END AS vasopressin_usage
FROM sepsis_patients AS sp
LEFT JOIN mimiciv_derived.vasopressin AS v USING (stay_id);


02.1 sepsis_patients的疾病的分布情况
---在此基础上和 mimiciv_hosp.diagnoses_icd 表与sepsis_patients对比（using stay_id）,查询icd_code in ('7100','7140','7200')

-- 4.1.从汇总表icd_cod查询icd_code '5715','5712'
SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE d_icd_diagnoses.icd_code ILIKE '%7100%'
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC;

02.2 sepsis_patients的疾病的分布情况：long_title ILIKE '%Ankylosing spondylitis%'	
-- 4.2.从汇总表long_title查询'%Cirrhosis of liver%'
SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE d_icd_diagnoses.long_title ILIKE '%Ankylosing spondylitis%'
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC;
02.3 结果：
'7100', Systemic lupus erythematosus
'M329', Systemic lupus erythematosus, unspecified
'7200', Ankylosing spondylitis
'M459', Ankylosing spondylitis of unspecified sites in spine
'340', Multiple sclerosis
'G35', Multiple sclerosis（多发性硬化症）
'7140', Rheumatoid arthritis（类风湿性关节炎）
'M069', Rheumatoid arthritis, unspecified（未明确的类风湿性关节炎）
'M0600', Rheumatoid arthritis without rheumatoid factor, unspecified site（未明确部位的无类风湿因子的类风湿性关节炎）
'71430', Polyarticular juvenile rheumatoid arthritis, chronic or unspecified（多关节型青少年类风湿性关节炎，慢性或未明确）
'5569', Ulcerative colitis, unspecified（未明确的溃疡性结肠炎）
'5568', Other ulcerative colitis（其他溃疡性结肠炎）
'K5190', Ulcerative colitis, unspecified, without complications（未明确的溃疡性结肠炎，无并发症）
'G7000',Myasthenia gravis without (acute) exacerbation（无（急性）恶化的重症肌无力）
'35800', Myasthenia gravis without (acute) exacerbation（无（急性）恶化的重症肌无力）
'57142', Autoimmune hepatitis（自身免疫性肝炎）
'E063', Autoimmune thyroiditis（自身免疫性甲状腺炎）
'7102',  Sicca syndromee（干燥综合征）
'M3500' Sicca syndromee（干燥综合征）
'6960', Psoriatic arthropathy（银屑病关节炎）
'4460', Polyarteritis nodosa（结节性多动脉炎）
'57142',Autoimmune hepatitis（自身免疫性肝炎）
'K754', Autoimmune hepatitis（自身免疫性肝炎）
'E063', Autoimmune thyroiditis

'V08',  Asymptomatic human immunodeficiency virus [HIV] infection status
'042', Asymptomatic human immunodeficiency virus [HIV] infection status
'20280',
'Z21', Asymptomatic human immunodeficiency virus [HIV] infection status
'B20', Asymptomatic human immunodeficiency virus [HIV] infection status
'20280',

'Z8572'Personal history of non-Hodgkin lymphomas
'C8339', Diffuse large B-cell lymphoma, extranodal and solid organ sites
'C8338' Diffuse large B-cell lymphoma, lymph nodes of multiple sites
'Z8571', Personal history of Hodgkin lymphoma
transplant
'E8780'
'V420'
'99681'
tumor
'65411'
'O3413'
cancer
'D701'
Lymphoma
'20280'


系统性红斑狼疮	7100
抗磷脂综合征	D6861  
银屑病关节病	6960
风湿病多发性肌痛	725
溃疡性结肠炎，未指明，无并发症	K5190  
多发性硬化症	340
脊柱未指明部位的强直性脊柱炎	M459   
系统性红斑狼疮并发心包炎	M3212  
溃疡性（慢性）直肠炎	5562
系统性硬化	7101
克罗恩病，未指明，无并发症	K5090  
人类免疫缺陷病毒病	B20    
多发性硬化症	G35    
溃疡性（慢性）直肠炎，无并发症	K5120  
其他皮肌炎，未明确涉及器官	M3310  
多动脉炎伴肺部受累[Churg Strauss]	M301   
多发性肌炎伴肌病	M3322  



E8780  	9	Surgical operation with transplant of whole organ causing abnormal patient reaction, or later complication, without mention of misadventure at time of operation
V420   	9	Kidney replaced by transplant
99681  	9	Complications of transplanted kidney
Z940   	10	Kidney transplant status
V422   	9	Heart valve replaced by transplant
V4983  	9	Awaiting organ transplant status
V427   	9	Liver replaced by transplant
Y830   	10	Surgical operation with transplant of whole organ as the cause of abnormal reaction of the patient, or of later complication, without mention of misadventure at the time of the procedure
Z944   	10	Liver transplant status
Z7682  	10	Awaiting organ transplant status
V4282  	9	Peripheral stem cells replaced by transplant
Z9484  	10	Stem cells transplant status
99682  	9	Complications of transplanted liver
V4283  	9	Pancreas replaced by transplant 

    WHERE dia.icd_code IN (
        '7100', 'M329', '7140', '7200', 'M459', '340', 'G35', '7140',
        'M069', 'M0600', '71430', '5569', '5568', 'K5190', 'G7000',
        '35800', '57142', 'E063', '7102', 'M3500', '6960', '4460',
        'K754', 'V08', '042', 'Z21', 'B20', 'Z8572', 'C8339', 'C8338',
        'Z8571', '7291', '7109', '725', 'M3212', 'L409', '7101', 'M349',
        'V08', '042', 'Z21', 'B20', 'M3219', 'M3218', 'M3217', 'Z8572', 'C8339', 'C8338', 'Z8571', 'E8780', 'V420', '99681', '65411', 'O3413', 'D701', '20280'
    )


03.filtered_immunpatients
---选择有免疫疾病的患者保存为immunpatients,这一步非常重要！！！需要找到所有的免疫疾病的icd_code
CREATE MATERIALIZED VIEW immunpatients AS
WITH filtered_patients AS (
    SELECT DISTINCT ICUD.hadm_id AS sp_hadm_id, d_icd_diagnoses.long_title
    FROM mimiciv_derived.age AS age,
         mimiciv_derived.icustay_detail AS ICUD,
         mimiciv_hosp.diagnoses_icd AS dia,
         mimiciv_hosp.d_icd_diagnoses AS d_icd_diagnoses
    WHERE dia.hadm_id = age.hadm_id 
      AND age.hadm_id = icud.hadm_id
      AND dia.icd_code IN ('7100', 'M329', '7200', 'M459', '7140', '5569', '5568', 'K5190', 'G7000', '35800', '57142', 'E063', '7102', 'M3500', '6960', '4460', 'K754', 'V08', '042', 'Z21', 'B20', 'Z8572', 'C8339', 'C8338', 'Z8571', '7291', '7109', '725', 'M3212', 'L409', '7101', 'M349', 'K5090', 'M3322', )

      AND age.age >= 10 AND age.age <= 100
      AND icud.first_icu_stay = TRUE
      AND dia.icd_code = d_icd_diagnoses.icd_code
)
SELECT fp.sp_hadm_id, fp.long_title, sp.*
FROM filtered_patients fp
RIGHT JOIN mimiciv_derived.sepsis_patients_vaso sp ON fp.sp_hadm_id = sp.hadm_id;

04.immudrug_data
---这里通过重新命名解决了无法保存为表格的问题！！！
 SELECT d.subject_id AS se_subject_id, d.hadm_id AS se_hadm_id, d.drug_IV, im.*,
---选择有免疫疾病的患者immunpatients，且注射了皮质醇激素的
---计算生存时间
CREATE VIEW sepsis_GCs AS
WITH drug_data AS (
    SELECT p.subject_id, p.hadm_id,
           MAX(CASE WHEN p.drug LIKE '%Dexamethasone%' OR p.drug LIKE '%Hydrocortisone%' OR p.drug LIKE '%Methylprednisolone%' THEN 1 ELSE 0 END) AS drug_IV
    FROM mimiciv_hosp.prescriptions AS p
    WHERE p.route = 'IV'
    GROUP BY p.subject_id, p.hadm_id
),
patient_data AS (
    SELECT d.subject_id, d.hadm_id, d.drug_IV, im.*,
    SELECT d.subject_id AS se_subject_id, d.hadm_id AS se_hadm_id, d.drug_IV, im.*,
           CASE WHEN im.dod IS NOT NULL THEN 1 ELSE 0 END AS death,
           CASE
               WHEN im.dod IS NOT NULL AND (im.dod - im.icu_outtime) <= INTERVAL '28 days' THEN 1
               ELSE 0
           END AS died_within_28_days,
           CASE WHEN im.dod IS NOT NULL THEN EXTRACT(DAY FROM (im.dod - im.icu_outtime)) ELSE NULL END AS survival_days
    FROM drug_data AS d
    JOIN mimiciv_derived.immunpatients AS im ON d.hadm_id = im.hadm_id
    WHERE im.first_icu_stay = True
)
SELECT *
FROM patient_data;




---在脓毒症患者中
--确诊为sepsis的患者
--查询18-60岁之间的患者
--查询患者的首次入ICU记录
--注射了皮质醇激素的
--(im.dod - im.icu_outtime) <= INTERVAL '28 days' 




SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC
LIMIT 10;

SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE d_icd_diagnoses.long_title ILIKE '%myocardial infarction%'
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC;


CREATE TABLE mimiciv_derived.d_icd_diagnoses_count AS
SELECT icd_code, icd_version, long_title, COUNT(icd_code) AS code_count
FROM mimiciv_hosp.d_icd_diagnoses
GROUP BY icd_code, icd_version, long_title;

CREATE TABLE mimiciv_derived.drug_count AS
SELECT
  medication,
  route,
  frequency,
  COUNT(medication) AS drug_count
FROM mimiciv_hosp.pharmacy
GROUP BY medication, route, frequency;


CREATE TABLE mimiciv_hosp.drug_count AS
SELECT
  subject_id,
  hadm_id,
  drug,
  COUNT(drug) AS drug_count
FROM prescriptions
GROUP BY subject_id, hadm_id, drug

CREATE TABLE mimiciv_hosp.drug_count AS
SELECT
  pharmacy_id,
  medication,
  route,
  frequency,
  COUNT( pharmacy_id,) AS drug_count
FROM mimiciv_hosp.pharmacy
GROUP BY pharmacy_id, medication, route, frequency

-- 从汇总表中获取所有药物的处方总数
SELECT
  drug_prescriptions.medication,
  drug_prescriptions.route,
  drug_prescriptions.frequency,
  COUNT(*) AS drug_count
FROM mimiciv_hosp.drug_count AS drug_prescriptions
-- 将 drug_count 表与 prescriptions 表进行连接
INNER JOIN mimiciv_hosp.prescriptions
ON drug_prescriptions.pharmacy_id = mimiciv_hosp.prescriptions.pharmacy_id
-- 按 medication, route, frequency 进行分组
GROUP BY drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency
ORDER BY drug_count DESC
LIMIT 10;

-- 从汇总表中获取所有药物的处方总数
SELECT drug_prescriptions.pharmacy_id, drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency, COUNT(drug_prescriptions.pharmacy_id) AS pharmacy_id_count
FROM mimiciv_derived.drug_count AS drug_prescriptions
-- 将drug_count表与prescriptions表进行连接
INNER JOIN mimiciv_hosp.prescriptions
ON drug_prescriptions.pharmacy_id = mimiciv_hosp.prescriptions.pharmacy_id
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE mimiciv_hosp.prescriptions.drug  ILIKE '%hydroxych%'
-- 按medication, route, frequency进行分组
GROUP BY drug_prescriptions.pharmacy_id, drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency
ORDER BY drug_count DESC
LIMIT 10;

-- 从汇总表中获取所有药物的处方总数
SELECT
  drug_prescriptions.medication,
  drug_prescriptions.route,
  drug_prescriptions.frequency,
  COUNT(*) AS drug_count
FROM mimiciv_hosp.drug_count AS drug_prescriptions
-- 将 drug_count 表与 prescriptions 表进行连接
INNER JOIN mimiciv_hosp.prescriptions
ON drug_prescriptions.pharmacy_id = mimiciv_hosp.prescriptions.pharmacy_id
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE mimiciv_hosp.prescriptions.drug  ILIKE '%hydroxych%'
-- 按 medication, route, frequency 进行分组
GROUP BY drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency
ORDER BY drug_count DESC
LIMIT 10;




SELECT drug_prescriptions.pharmacy_id, drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency, COUNT(drug_prescriptions.pharmacy_id) AS drug_count
FROM mimiciv_hosp.drug_count AS drug_prescriptions
-- 将 drug_count 表与 prescriptions 表进行连接
INNER JOIN mimiciv_hosp.prescriptions
ON drug_prescriptions.pharmacy_id = mimiciv_hosp.prescriptions.pharmacy_id
-- 按 medication, route, frequency 进行分组
GROUP BY drug_prescriptions.pharmacy_id, drug_prescriptions.medication, drug_prescriptions.route, drug_prescriptions.frequency
ORDER BY drug_count DESC
LIMIT 10;


SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC
LIMIT 10;

SELECT d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title, COUNT(diagnoses_icd.icd_code) AS code_count
-- 从d_icd_diagnoses_count表中选择icd_code, icd_version, long_title以及满足条件的诊断总数
FROM mimiciv_derived.d_icd_diagnoses_count AS d_icd_diagnoses
-- 将d_icd_diagnoses_count表与diagnoses_icd表进行连接
INNER JOIN mimiciv_hosp.diagnoses_icd
ON d_icd_diagnoses.icd_code = diagnoses_icd.icd_code
-- 过滤出long_title包含'myocardial infarction'的记录
WHERE d_icd_diagnoses.long_title ILIKE '%Transplan%'
-- 按icd_code, icd_version, long_title进行分组
GROUP BY d_icd_diagnoses.icd_code, d_icd_diagnoses.icd_version, d_icd_diagnoses.long_title
-- 按诊断总数倒序排序
ORDER BY code_count DESC;