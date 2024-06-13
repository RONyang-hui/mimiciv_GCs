DROP TABLE IF EXISTS cohort_imm;
CREATE TABLE cohort_imm AS
SELECT *
FROM (
    SELECT DISTINCT *,
        CASE WHEN (population_im.drug_flag_stain = 0 OR population_im.drug_flag_insulin = 0) THEN 1 ELSE 0 END AS drug_flag
    FROM immune_group
    NATURAL LEFT JOIN population_im 
    NATURAL LEFT JOIN (
        SELECT subject_id, hadm_id, stay_id, los
        FROM mimiciv_icu.icustays
    ) AS icu
) AS subquery

WHERE drug_flag = 0 
    AND age >= 18;
    AND icu_order = 1
    AND icu.los >= 2;



    