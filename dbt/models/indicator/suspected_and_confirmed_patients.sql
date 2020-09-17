with patients as (
    select patient_id, age_in_years, owner_name, case_status, created_date
    from {{ref('patient_info')}}
),
labs as (
    select distinct on (covid_patient_id)
        covid_patient_id,
        analysis_date,
        collection_date,
        final_lab_result
    from
        {{ref('lab_results')}}
    order by 
        covid_patient_id, analysis_date
),
symptoms as (
    select distinct on (covid_patient_id)
        covid_patient_id,
        symptomatic, 
        symptomatic_onset_date, 
        reported_date as symptomatic_reported_date
    from
        {{ref('symptom_monitoring')}}
    order by 
        covid_patient_id, reported_date
),
case_investigation as (
    select distinct on (patient_id)
        case_investigation.patient_id,
        created_date,
        interview_date
    from 
        {{ref('case_investigation')}}, {{ref('patient_info')}}
    where
        case_investigation.patient_id=patient_info.patient_id
    order by
        case_investigation.patient_id, interview_date
),
num_contacts as (
    select 
        patient_id,
        count(*) as number_of_contacts
    from 
        {{ref('patient_info')}}
    inner join 
        {{ref('contact_info')}} on {{ref('contact_info')}}.index_patient_id={{ref('patient_info')}}.patient_id
    group by 
        {{ref('patient_info')}}.patient_id
),
final as (
    select 
        patients.patient_id,
        patients.created_date,
        interview_date as first_contacted_date,
        case when number_of_contacts is null then 0 else number_of_contacts end as number_of_contacts,
        age_in_years,
        owner_name,
        case
            when labs.analysis_date is null then  
                null
            when DATE_PART('day',case_investigation.interview_date::timestamp - labs.analysis_date::timestamp) < 2 then
                TRUE
            else
                FALSE
        end as contacted_within_2days,
        case
            when labs.collection_date is null then  
                null
            when DATE_PART('day',case_investigation.created_date::timestamp - labs.collection_date::timestamp) < 2 then
                TRUE
            else
                FALSE
        end as case_registered_within_2days_sample_collection,
        case
            when symptomatic_onset_date is null or labs.collection_date is null then 
                null
            when DATE_PART('day',labs.collection_date::timestamp - symptomatic_onset_date::timestamp) < 4 then 
                TRUE
            else
                FALSE
        end as symptomatic_tested_under_4days,
        final_lab_result,
        case 
            when final_lab_result = 'positive' then 'confirmed'
            else case_status
        end as merged_case_status,
        symptomatic, 
        symptomatic_onset_date, 
        symptomatic_reported_date
    from
        patients
    left join case_investigation 
        on patients.patient_id=case_investigation.patient_id
    left join labs
        on patients.patient_id=labs.covid_patient_id
    left join symptoms
        on patients.patient_id=symptoms.covid_patient_id
    left join num_contacts
        on patients.patient_id=num_contacts.patient_id
    where 
        labs.final_lab_result is null or labs.final_lab_result <> 'negative'
)
select * from final