with source as (

    select * from {{ source('adobe_webtracking', 'stage_datafeeds_mobile_attributes') }}

),

renamed as (

    select
        job_instance_id,
        mobile_id,
        manufacturer,
        mobile_device_name,
        mobile_device_type,
        mobile_os,
        mobile_diagonal_screen_size,
        mobile_screen_hight,
        mobile_screen_width

    from source

)

select * from renamed