with contact as (
    select
        caseid as contact_id,
        "indices.covid_19_case" as index_patient_id,
        opened_date as created_date,
        age_in_years,
        owner_name
    from 
        case_contact
),
final as (
    select 
        contact_id,
        index_patient_id,
        created_date,
        owner_name,
        age_in_years
    from
        contact
)
select * from final
