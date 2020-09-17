with a0 as (
    select
        "form.case.@case_id" as covid_patient_id,
        case  
        	WHEN "ymptoms.date_of_first_symptom_onset.date_of_first_symptom_onset" IS NULL THEN FALSE ELSE TRUE
        end as symptomatic,
        "ymptoms.date_of_first_symptom_onset.date_of_first_symptom_onset" as symptomatic_onset_date,
        "_information.data_collector_information.form_completion_date_a0" as reported_date
    from 
        form_a0
),
a1 as (
    select
        "form.case.@case_id" as covid_patient_id,
        case 
            WHEN "ient_symptoms.date_of_first_symptom.date_of_first_symptom_onset" IS NULL THEN FALSE ELSE TRUE
        end as symptomatic,
        "ient_symptoms.date_of_first_symptom.date_of_first_symptom_onset" as symptomatic_onset_date,
        "rmation.data_collector_information_list.form_completion_date_a1" as reported_date
    from 
        form_a1
),
a2 as (
    select
        "form.case.@case_id" as covid_patient_id,
        -- case 
        --     WHEN "ient_symptoms.date_of_first_symptom.date_of_first_symptom_onset" IS NULL THEN FALSE ELSE TRUE
        -- end as symptomatic,
        -- "ient_symptoms.date_of_first_symptom.date_of_first_symptom_onset" as symptomatic_onset_date,
        "_information.data_collector_information.form_completion_date_a2" as reported_date
    from 
        form_a2
),
b1 as (
    select
        "form.subcase_0.case.@case_id" as contact_id,
        case 
            WHEN "ntact.date_time_first_symptom_onset.date_of_first_symptom_onset" IS NULL THEN FALSE ELSE TRUE
        end as symptomatic,
        "ntact.date_time_first_symptom_onset.date_of_first_symptom_onset" as symptomatic_onset_date,
        completed_time as reported_date
    from 
        form_b1
),
b2 as (
    select
        "form.case.@case_id" as contact_id,
        case 
            WHEN "ntact.date_time_first_symptom_onset.date_of_first_symptom_onset" IS NULL THEN FALSE ELSE TRUE
        end as symptomatic,
        "ntact.date_time_first_symptom_onset.date_of_first_symptom_onset" as symptomatic_onset_date,
        completed_time as reported_date
    from 
        form_b2
),
final as (
    select 
        covid_patient_id,
        NULL as contact_id,
        symptomatic,
        symptomatic_onset_date,
        reported_date
    from
        a0 
    UNION ALL
    select 
        covid_patient_id,
        NULL as contact_id,
        symptomatic,
        symptomatic_onset_date,
        reported_date
    from
        a1 
    UNION ALL
    select 
        covid_patient_id,
        NULL as contact_id,
        NULL as symptomatic,
        NULL as symptomatic_onset_date,
        reported_date
    from
        a2
    UNION ALL
    select 
        NULL as covid_patient_id,
        contact_id,
        symptomatic,
        symptomatic_onset_date,
        reported_date
    from
        b1 
    UNION ALL
    select 
        NULL as covid_patient_id,
        contact_id,
        symptomatic,
        symptomatic_onset_date,
        reported_date
    from
        b2
)
select * from final