DROP TABLE IF EXISTS population_im ;
CREATE TABLE population_im AS
WITH immune_group AS (
    SELECT DISTINCT dia.subject_id
    FROM mimiciv_hosp.diagnoses_icd dia
    LEFT JOIN mimiciv_hosp.diagnoses_icd_name p
    USING (subject_id)
    WHERE dia.icd_code IN (
        '7100', 'M329', '7140', '7200', 'M459', '340', 'G35', '7140',
        'M069', 'M0600', '71430', '5569', '5568', 'K5190', 'G7000',
        '35800', '57142', 'E063', '7102', 'M3500', '6960', '4460',
        'K754', 'V08', '042', 'Z21', 'B20', 'Z8572', 'C8339', 'C8338',
        'Z8571', '7291', '7109', '725', 'M3212', 'L409', '7101', 'M349',
        'V08', '042', 'Z21', 'B20', 'M3219', 'M3218', 'M3217', 'Z8572', 'C8339', 'C8338', 'Z8571', 'E8780', 'V420', '99681', '65411', 'O3413', 'D701', '20280'
    )

    ---补充多余的病人
		AND dia.icd_code NOT IN ('25000', 'E119', 'E1122', '5770', 'K8590', 'K8590')
    GROUP BY dia.subject_id
),
icu_age_raw AS (
    SELECT stay_id, admission_age AS age
    FROM mimiciv_derived.icustay_detail
),
icu_age AS (
    SELECT stay_id,
        CASE WHEN age >= 130 THEN 91.5 ELSE age END AS age
    FROM icu_age_raw
),
icu_order AS (
    SELECT stay_id,
        RANK() OVER (PARTITION BY subject_id ORDER BY intime) AS icu_order
    FROM mimiciv_icu.icustays
),
drug_exclude AS (
    SELECT DISTINCT p.subject_id,
        CASE WHEN p.drug LIKE '%stain%' THEN 1 ELSE 0 END AS drug_flag_stain,
        CASE WHEN p.drug LIKE '%Insulin%' THEN 1 ELSE 0 END AS drug_flag_insulin
    FROM mimiciv_hosp.prescriptions p
    GROUP BY p.subject_id, p.drug
    HAVING MAX(CASE WHEN p.drug LIKE '%stain%' THEN 1 ELSE 0 END) = 1
        OR MAX(CASE WHEN p.drug LIKE '%Insulin%' THEN 1 ELSE 0 END) = 1
				AND p.drug IS NOT NULL
),

population_im AS (
    SELECT DISTINCT ON (stay_id) a.stay_id, a.subject_id, a.hadm_id, a.first_careunit, a.intime, a.outtime, icu_age.age, icu_order.icu_order,
        dru.drug_flag_stain, dru.drug_flag_insulin
    FROM (SELECT DISTINCT stay_id, subject_id, hadm_id, first_careunit, intime, outtime FROM mimiciv_icu.icustays) a
    NATURAL LEFT JOIN immune_group imm
    NATURAL LEFT JOIN icu_age 
    NATURAL LEFT JOIN icu_order 
    NATURAL LEFT JOIN drug_exclude dru
)
SELECT * FROM population_im;

---在生成population的过程中，需要看是哪些因素组成的，因为是基于要分析的队列，所以第一步就直接包括不需要纳入的人
