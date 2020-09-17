with a0 as (
    select
        "form.case.@case_id" as patient_id,
        "_information.data_collector_information.form_completion_date_a0" as interview_date
    from 
        form_a0
),
a1 as (
    select
        "form.case.@case_id" as patient_id,
        "rmation.data_collector_information_list.form_completion_date_a1" as interview_date
    from 
        form_a1
),
a2 as (
    select
        "form.case.@case_id" as patient_id,
        "_information.data_collector_information.form_completion_date_a2" as interview_date
    from 
        form_a2
),
final as (
    select 
        patient_id,
        interview_date
    from
        a0 
    UNION ALL
    select 
        patient_id,
        interview_date
    from
        a1 
    UNION ALL
    select 
        patient_id,
        interview_date
    from
        a2
)
select * from final