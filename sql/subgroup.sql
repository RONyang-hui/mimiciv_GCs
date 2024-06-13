with norepinephrine_cv as (
    select icustay_id, amount, charttime
    from inputevents_cv
    where itemid in (30047,30120)
)

, norepinephrine_mv as (
    select icustay_id, amount, starttime, endtime
    from inputevents_mv
    where itemid in (221906)
)

, norepinephrine as (
    select co.icustay_id, coalesce(mv.amount, cv.amount, 0) as amount
    from cohort co
    left join norepinephrine_mv mv on co.icustay_id = mv.icustay_id
        and mv.starttime between co.intime and co.outtime
    left join norepinephrine_cv cv on co.icustay_id = cv.icustay_id
        and cv.charttime between co.intime and co.outtime
)

, norepinephrine_max as (
    select icustay_id, max(amount) as norepinephrine_max
    from norepinephrine
    group by icustay_id
)

, dobutamine_cv as (
    select icustay_id, amount, charttime
    from inputevents_cv
    where itemid in (30042,30306)
)

, dobutamine_mv as (
    select icustay_id, amount, starttime, endtime
    from inputevents_mv
    where itemid in (221653)
)


, dobutamine as (
    select co.icustay_id, coalesce(mv.amount, cv.amount) as amount
    from cohort co
    left join dobutamine_mv mv on co.icustay_id = mv.icustay_id
        and mv.starttime between co.intime and co.outtime
    left join dobutamine_cv cv on co.icustay_id = cv.icustay_id
        and cv.charttime between co.intime and co.outtime
)

, dobutamine_flag as (
    select icustay_id,
        case when sum(amount) is not null then 1 else 0 end as dobutamine_flag
    from dobutamine
    group by icustay_id
)

, vasofree_0 as (
    select icustay_id, starttime, 
        case when (co.intime + interval '28' day) <= endtime then (co.intime + interval '28' day) else endtime end as endtime,
        co.outtime
    from merged_data co
    left join vasopressordurations vs using (icustay_id)
)

, vasofree_1 as (
    select icustay_id, starttime,
        case when outtime <= endtime then outtime else endtime end as endtime
    from vasofree_0
)

, vasofree_2 as (
    select icustay_id, extract(epoch from endtime - starttime) / 60.0 / 60.0 as duration_hours
    from vasofree_1
)

, vasofree_3 as (
    select icustay_id, sum(duration_hours) / 24.0 as vasoduration
    from vasofree_2
    group by icustay_id
)

, vasofree_4 as (
    select icustay_id, 28 - vasoduration as vasofreeday28
    from vasofree_3
)

, vasofree as (
    select icustay_id,
        coalesce(case when mort_28_day = 0 then vasofreeday28
                      else 0 end, 28) as vasofreeday28
    from merged_data co
    left join vasofree_4 using (icustay_id)
)

, ventfree_0 as (
    select icustay_id, starttime,
        case when (co.intime + interval '28' day) <= endtime then (co.intime + interval '28' day) else endtime end as endtime,
        co.outtime
    from merged_data co
    left join ventdurations ve using (icustay_id)
)

, ventfree_1 as (
    select icustay_id, starttime,
        case when outtime <= endtime then outtime else endtime end as endtime
    from ventfree_0
)

, ventfree_2 as (
    select icustay_id, extract(epoch from endtime - starttime) / 60.0 / 60.0 as duration_hours
    from ventfree_1
)

, ventfree_3 as (
    select icustay_id, sum(duration_hours) / 24.0 as ventduration
    from ventfree_2
    group by icustay_id
)

, ventfree_4 as (
    select icustay_id, 28 - ventduration as ventfreeday28
    from ventfree_3
)

, ventfree as (
    select icustay_id,
        coalesce(case when mort_28_day = 0 then ventfreeday28
                      else 0 end, 28) as ventfreeday28
    from merged_data co
    left join ventfree_4 using (icustay_id)
)

, sofa_2 as (
    select co.icustay_id,
        case when co.deathtime between (co.intime + interval '1' day) and (co.intime + interval '2' day) then 24
        else sf.sofa end as sofa
    from merged_data co
    left join sofasecond sf using (icustay_id)
)

, sofa_3 as (
    select co.icustay_id,
        case when co.deathtime between (co.intime + interval '2' day) and (co.intime + interval '3' day) then 24
        else sf.sofa end as sofa
    from merged_data co
    left join sofathird sf using (icustay_id)
)

, sofa_3_days as (
    select *
    from (select icustay_id from cohort) co
    natural left join (select icustay_id, sofa as sofa_1 from sofa) s1
    natural left join (select icustay_id, sofa as sofa_2 from sofa_2) s2
    natural left join (select icustay_id, sofa as sofa_3 from sofa_3) s3
)

, sofa_drop as (
    select icustay_id, sofa_1 as sofa, sofa_1 - sofa_2 as sofa_drop_2, sofa_1 - sofa_3 as sofa_drop_3
    from sofa_3_days
)

, subgroup as (
    select *
    from (select icustay_id, hadm_id, echo from cohort) co
    natural left join norepinephrine_max
    natural left join dobutamine_flag
    natural left join vasofree
    natural left join ventfree
    natural left join sofa_drop
)

select * from subgroup;


在亚组分析的时候，先进行数据的分层，分层的时候要注意，
1.选定可以进行分组的变量，这个变量的选择可以基于前面的forest的结果进行；
2.利用cut函数进行分层，分层之后利用library(jstable)进行亚组的分析，可以汇总到一个df中
3.在进行亚组分析的时候，分类的变量应该是num的形式，否则会报错
4.得到res之后的结果，这个结果不需要另存为csv也能直接使用（除非你是细节控，需要修改各种大小写等信息），当然如果你需要HR(95%CI)这种信息，还是需要自己添加一下的。
我们添加个空列用于显示可信区间，并把不想显示的NA去掉即可，还需要把P值，可信区间这些列变为数值型。
install.packages("jstable")

## From github: latest version
remotes::install_github('jinseob2kim/jstable')
library(jstable)



df_cox <- data.frame(
  age_group = cut(full_data$age, breaks = c(0, 65, Inf), labels = c("<=65", ">65")),
  weight_group = cut(full_data$weight, breaks = c(0, 50, 100, Inf), labels = c("<=50", "51-100", ">100")),
  vs_map = cut(full_data$vs_map_first, breaks = c(0, 50, 100, Inf), labels = c("Low", "Medium", "High")),
  lab_platelet = cut(full_data$lab_platelet_first, breaks = c(0, 100, 200, Inf), labels = c("Low", "Medium", "High")),
  vs_heart_rate = cut(full_data$vs_heart_rate_first, breaks = c(0, 60, 100, Inf), labels = c("Low", "Normal", "High")),
  lab_bun = cut(full_data$lab_bun_first, breaks = c(0, 20, 40, Inf), labels = c("Low", "Medium", "High")),
  lab_wbc = cut(full_data$lab_wbc_first, breaks = c(0, 10, 20, Inf), labels = c("Low", "Medium", "High"), include.lowest = TRUE),
  sapsii_group = cut(full_data$sapsii, breaks = c(0, 20, 40, 60, Inf), labels = c("<=20", "21-40", "41-60", ">60")),
  lab_hemoglobin_first_group = cut(full_data$lab_hemoglobin_first, breaks = c(-Inf, 9.43, Inf), labels = c("<=9.43", ">9.43")),
  lab_potassium_first_group = cut(full_data$lab_potassium_first, breaks = c(-Inf, 4.39, Inf), labels = c("<=4.39", ">4.39")),
  lab_chloride_first_group = cut(full_data$lab_chloride_first, breaks = c(-Inf, 102.92, Inf), labels = c("<=102.92", ">102.92")),
  survival_days = full_data$survival_days,
  event_state = full_data$event_state,
  mort_28_day = full_data$mort_28_day,
  gcs = full_data$gcs
)
View(df_cox)
data.table::fwrite(df_cox, file.path(data_dir, "df_cox.csv"), col.names = TRUE)

df_cox$event_state <- as.numeric(df_cox$event_state)
df_cox$mort_28_day <- as.numeric(df_cox$mort_28_day)

res <- TableSubgroupMultiCox(
  library(jstable)
  # 指定公式
  formula = Surv(survival_days, event_state) ~ gcs, 
  
  # 指定哪些变量有亚组
  var_subgroups = c("age_group","weight_group","vs_map","lab_platelet","vs_heart_rate", "lab_hemoglobin_first_group", "lab_potassium_first_group", "lab_chloride_first_group",
                    "lab_bun","lab_wbc","sapsii_group"), 
  data = df_cox #指定你的数据
  )
res


res <- TableSubgroupMultiCox(
  
  # 指定公式
  formula = Surv(survival_days, mort_28_day) ~ gcs, 
  
  # 指定哪些变量有亚组
  var_subgroups = c("age_group","weight_group","vs_map","lab_platelet","vs_heart_rate", "lab_hemoglobin_first_group", "lab_potassium_first_group", "lab_chloride_first_group",
                    "lab_bun","lab_wbc","sapsii_group"), 
  data = df_cox #指定你的数据
  )
res
data.table::fwrite(res, file.path(data_dir, "res.csv"), col.names = TRUE)
这段R代码首先将res赋值给plot_df。


这项Cox回归分析旨在探究多个变量对ICU患者生存结局（死亡或生存）的影响，其中1代表死亡，0代表生存。总体来看，研究涉及4401名患者，主要结果如下：

总体分析：没有单一变量显示出对生存结局有显著影响（Point Estimate=0.95，95% CI: 0.73-1.24，P=0.702）。这意味着，从整体样本看，研究的变量与患者生存率提高或降低无明显关联。

年龄分组：年龄≤65岁和>65岁的患者在生存率上无显著差异（P=0.961）。

体重分组：虽然大部分体重组别间无显著差异，但体重>100kg的患者风险比（HR）较高（Point Estimate=1.25），提示高体重可能与较差的预后相关（P=0.738，尽管P值显示不显著，但仍需注意这一趋势）。

血压（vs_map）：不同血压水平的患者间生存结局无显著差异（P=0.894）。

血小板计数（lab_platelet）：血小板计数与生存结局显著相关，血小板计数高的患者（High组）具有更好的生存预期（Point Estimate=0.52，P=0.019），而血小板计数低的患者风险增加（Low组，Point Estimate=1.43，P=0.106）。

心率（vs_heart_rate）：心率的不同分类与生存结局无显著相关性（P=0.467）。

首次血红蛋白水平（lab_hemoglobin_first）：血红蛋白水平>9.43g/dL的患者生存率显著提高（Point Estimate=0.55，P=0.006），提示较高的血红蛋白水平与较好的生存率相关。

其他实验室指标（如钾离子、氯离子、BUN、WBC计数）：除血小板外，仅白细胞计数（lab_wbc）显示出中等程度的显著性（P=0.044），白细胞计数低的患者生存结局较好。

SAPS II评分分组：不同疾病严重程度（根据SAPS II评分）的患者间，生存结局无显著差异（P=0.618）。

综上所述，血小板计数和首次血红蛋白水平是与ICU患者生存结局显著相关的两个重要变量，而高体重和低白细胞计数也显示出一定的趋势，提示在这些因素上的进一步研究可能有助于理解患者预后并指导治疗策略。




plot_df <- res
plot_df[,c(2,3,9)][is.na(plot_df[,c(2,3,9)])] <- " "
plot_df$` ` <- paste(rep(" ", nrow(plot_df)), collapse = " ")
plot_df[,4:6] <- apply(plot_df[,4:6],2,as.numeric)


library(forestploter)
library(grid)

p <- forest(
  data = plot_df[,c(1,2,3,11,9)],
  lower = plot_df$Lower,
  upper = plot_df$Upper,
  est = plot_df$`Point Estimate`,
  ci_column = 4,
  #sizes = (plot_df$estimate+0.001)*0.3, 
  ref_line = 1, 
  xlim = c(0.1,4)
  )
print(p)

它将plot_df中第2列、第3列和第7列中的所有NA值替换为一个空格字符串" "。
接着，它在plot_df中创建了一个新的列，列名为一个空格" "，所有的值都是一个空格字符串" "。
最后，它将plot_df中第4列到第6列的所有值转换为数值类型。这是通过apply函数实现的，该函数对数据框的每一列（由于MARGIN=2）应用as.numeric函数。

plot_df <- res
plot_df[,c(2,3,7)][is.na(plot_df[,c(2,3,7)])] <- " "
plot_df$` ` <- paste(rep(" ", nrow(plot_df)), collapse = " ")
plot_df[,4:6] <- apply(plot_df[,4:6],2,as.numeric)

library(forestploter)
library(grid)

p <- forest(
  data = plot_df[,c(1,2,3,9,7)],
  lower = plot_df$Lower,
  upper = plot_df$Upper,
  est = plot_df$`Point Estimate`,
  ci_column = 4,
  #sizes = (plot_df$estimate+0.001)*0.3, 
  ref_line = 1, 
  xlim = c(0.1,4)
  )
print(p)


然后，
plot_df <- res
plot_df[,c(2,3,9)][is.na(plot_df[,c(2,3,9)])] <- " "
plot_df$` ` <- paste(rep(" ", nrow(plot_df)), collapse = " ")
plot_df[,4:6] <- apply(plot_df[,4:6],2,as.numeric)



根据你提供的数据结果，我们可以得出以下总结：

该数据研究涉及到两个组别，即未使用激素组和使用激素组。在未使用激素组中，平均年龄为65.34岁，其中55.2%为女性，平均体重为80.07公斤。而在使用激素组中，平均年龄为65.29岁，其中57.1%为女性，平均体重为81.12公斤。

在评估患者的疾病严重程度方面，使用了SAPS II评分和24小时内的SOFA评分。结果显示，未使用激素组的平均SAPS II评分为39.23，使用激素组的平均评分为36.54。在24小时内的SOFA评分方面，未使用激素组的平均评分为3.78，使用激素组的平均评分为3.51。这表明使用激素的组别在疾病严重程度方面略低于未使用激素的组别。

在28天的死亡率方面，未使用激素组的死亡率为16.1%，而使用激素组的死亡率为9.6%。这表明使用激素可能与较低的死亡率相关。

关于生存期，未使用激素组的平均存活天数为368.63天，而使用激素组的平均存活天数为276.37天。这表明未使用激素组的患者可能具有更长的生存期。

另外，对于各种生理指标的平均值，两个组别之间也存在一些差异。例如，使用激素组的初始动脉压、心率和乳酸浓度略低于未使用激素组；然而，使用激素组的血小板计数和血钠浓度略低于未使用激素组。

总体而言，根据提供的数据，使用激素的组别在某些方面似乎具有较低的疾病严重程度、较低的死亡率和较短的生存期。然而，这只是初步观察结果，具体的因果关系还需要进一步的研究和分析来得出准确的结论。


