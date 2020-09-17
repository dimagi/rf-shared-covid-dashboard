with patient as (
    select
        caseid as patient_id,
        opened_date as created_date,
        patient_case_status as case_status,
        owner_name,
        age_in_years
    from 
        case_covid
),
final as (
    select 
        patient_id,
        created_date,
        case_status,
        owner_name,
        age_in_years
    from
        patient
)
select * from final