{{
    config(
        materialized = 'table',
        tags = ["daily"],
    )
}}

with source as (
    select * 
    from {{ source('open_brewery_db', 'raw_breweries') }}
),

final as (
    select
        *,
        -- Convert fetched_at to DATETIME and fallback to macro value
        coalesce(
            safe_cast(fetched_at as datetime),
            {{ get_refreshed_at() }}
        ) as fetched_at_dt,

        -- Extract date from DATETIME
        date(
            coalesce(
                safe_cast(fetched_at as datetime),
                {{ get_refreshed_at() }}
            )
        ) as fetched_date
    from source
),

renamed_and_cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['id', 'fetched_date']) }} as brewery_unique_key,
        cast(id as string) as brewery_pk,
        name as brewery_name,
        brewery_type,
        fetched_date,
        
        -- Address fields
        address_1 as street_address_1,
        nullif(address_2, '') as street_address_2,
        nullif(address_3, '') as street_address_3,
        city,
        state_province,
        country,
        trim(postal_code) as postal_code,
        
        -- Geolocation
        safe_cast(latitude as numeric) as latitude,
        safe_cast(longitude as numeric) as longitude,
        
        -- Contact Info
        phone,
        website_url,
        
        -- Metadata
        fetched_at_dt as extracted_at_dt
    from final
)

select *
from renamed_and_cleaned
qualify row_number() over (
    partition by brewery_pk, fetched_date
    order by extracted_at_dt desc
) = 1
