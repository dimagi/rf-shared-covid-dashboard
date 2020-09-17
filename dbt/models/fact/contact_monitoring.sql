with b1 as (
    select
        "form.subcase_0.case.@case_id" as contact_id,
        completed_time as interview_date
    from 
        form_b1
),
b2 as (
    select
        "form.case.@case_id" as contact_id,
        completed_time as interview_date
    from 
        form_b2
),
final as (
    select 
        contact_id,
        interview_date
    from
        b1 
    UNION ALL
    select 
        contact_id,
        interview_date
    from
        b2
)
select * from final