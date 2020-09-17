with contacts as (
    select contact_id, age_in_years, owner_name
    from {{ref('contact_info')}}
),
labs as (
    select
        contact_id,
        analysis_date,
        collection_date,
        final_lab_result
    from
        {{ref('lab_results')}}
),
symptoms as (
    select
        contact_id,
        symptomatic, 
        symptomatic_onset_date, 
        reported_date as symptomatic_reported_date
    from
        {{ref('symptom_monitoring')}}
),
contact_monitoring as (
    select distinct on (contact_monitoring.contact_id)
        contact_info.contact_id,
        created_date,
        interview_date
    from 
        {{ref('contact_monitoring')}}, {{ref('contact_info')}}
    where
        contact_monitoring.contact_id=contact_info.contact_id
    order by
        contact_monitoring.contact_id, interview_date
),
final as (
    select 
        contacts.contact_id,
        created_date,
        interview_date as first_contacted_date,
        age_in_years,
        owner_name,
        case
            when labs.analysis_date is null then
                null
            when DATE_PART('day',contact_monitoring.interview_date::timestamp - labs.analysis_date::timestamp) < 2 then
                TRUE
            else
                FALSE
        end as contacted_within_2days,
        case
            when symptomatic_onset_date is null or labs.collection_date is null then 
                null
            when DATE_PART('day',labs.collection_date::timestamp - symptomatic_onset_date::timestamp) < 4 then 
                TRUE
            else
                FALSE
        end as symptomatic_tested_under_4days,
        case
            when final_lab_result is null then 'unknown'
            else final_lab_result
        end as final_lab_result,
        symptomatic, 
        symptomatic_onset_date, 
        symptomatic_reported_date,
        case 
            when interview_date >= symptomatic_onset_date then
                'symptomatic_before_interview'
            when interview_date < symptomatic_onset_date then
                'symptomatic_during_monitoring'
            when symptomatic = FALSE then
                'asymptomatic'
            else    
                null
        end as contact_cascade_label
    from
        contacts
    left join contact_monitoring
        on contacts.contact_id=contact_monitoring.contact_id
    left join labs
        on contacts.contact_id=labs.contact_id
    left join symptoms
        on contacts.contact_id=symptoms.contact_id
    -- where 
    --     labs.final_lab_result is null or labs.final_lab_result <> 'negative'
)
select * from final