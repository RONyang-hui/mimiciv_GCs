-- 之前生成的vital_signs是COPD的队列依赖生成的，所以需要重新生成
drop materialized view if exists vitalsigns CASCADE;
create materialized view vitalsigns as
with vital_signs as (
    select stay_id,
           charttime,
           case when itemid in (456,52,6702,443,220052,220181,225312) then 'map'
                when itemid in (223762,676,223761,678) then 'temp'
                when itemid in (211,220045) then 'heart_rate'
                when itemid in (113,1103,220074) then 'cvp'
            else null end as label,

            case when itemid in (223761,678) and ((valuenum-32)/1.8)<10 then null
                 when itemid in (456,52,6702,443,220052,220181,225312) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (211,220045) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (223762,676) and valuenum < 10 then null
                 -- convert F to C
                 when itemid in (223761,678) then (valuenum-32)/1.8
                 -- sanity checks on data - one outliter with spo2 < 25
                 when itemid in (646,220277) and valuenum <= 25 then null
            else valuenum end as valuenum
    from mimiciv_icu.chartevents

)
, vital_signs_cohort as (
    select v.stay_id, v.charttime, v.label, v.valuenum
    from cohort_tyg c
    left join vital_signs v using (stay_id)
    where v.charttime between c.intime and c.intime + interval '1 day'
          and v.charttime between c.intime and c.outtime
          and v.label is not null
          and v.valuenum is not null
)

select * from vital_signs_cohort;





-- 之前生成的vital_signs是COPD的队列依赖生成的，所以需要重新生成
drop materialized view if exists vital_sign CASCADE;
create materialized view vital_sign as
with vital_sign as (
    select stay_id,
           charttime,
           case when itemid in (456,52,6702,443,220052,220181,225312) then 'map'
                when itemid in (223762,676,223761,678) then 'temp'
                when itemid in (211,220045) then 'heart_rate'
                when itemid in (113,1103,220074) then 'cvp'
            else null end as label,

            case when itemid in (223761,678) and ((valuenum-32)/1.8)<10 then null
                 when itemid in (456,52,6702,443,220052,220181,225312) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (211,220045) and (valuenum <= 0 or valuenum >= 300) then null
                 when itemid in (223762,676) and valuenum < 10 then null
                 -- convert F to C
                 when itemid in (223761,678) then (valuenum-32)/1.8
                 -- sanity checks on data - one outliter with spo2 < 25
                 when itemid in (646,220277) and valuenum <= 25 then null
            else valuenum end as valuenum
    from mimiciv_icu.chartevents

)
select * from vital_sign;

drop materialized view if exists vital_signs_cohort_gcs CASCADE;
create materialized view vital_signs_cohort_gcs as

    select v.stay_id, v.charttime, v.label, v.valuenum
    from cohort_gcs c
    left join vital_sign v using (stay_id)
    where v.charttime between c.icu_intime and c.icu_intime + interval '1 day'
          and v.charttime between c.icu_intime and c.icu_outtime
          and v.label is not null
          and v.valuenum is not null

