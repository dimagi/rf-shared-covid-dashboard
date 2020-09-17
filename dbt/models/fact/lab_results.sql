with covid_virology as (
    select
        "indices.covid_19_case" as covid_patient_id,
        NULL as contact_id,
        date_sample_collected as collection_date,
        result_date as analysis_date,
        result as final_lab_result
    from
        case_virology
),
contact_serology as (
    select
        NULL as covid_patient_id,
        "indices.contact" as contact_id,
        date_sample_collected as collection_date,
        result_date as analysis_date,
        result as final_lab_result
    from 
        case_contact_serology
),
contact_virology as (
    select
        NULL as covid_patient_id,
        "indices.contact" as contact_id,
        date_sample_collected as collection_date,
        result_date as analysis_date,
        result as final_lab_result
    from 
        case_contact_virology
),
final as (
    select 
        covid_patient_id,
        contact_id,
        collection_date,
        analysis_date,
        final_lab_result
    from
        contact_serology 
    UNION ALL
    select 
        covid_patient_id,
        contact_id,
        collection_date,
        analysis_date,
        final_lab_result
    from
        contact_virology 
    UNION ALL
    select 
        covid_patient_id,
        contact_id,
        collection_date,
        analysis_date,
        final_lab_result
    from
        covid_virology 
)
select * from final