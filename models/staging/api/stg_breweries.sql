{{ 
    config({
    'materialized': 'view',
    'tags': ["daily"],
    })
}}

with source as (

    -- Reference the raw table
    select * from {{ source('open_brewery_db', 'raw_breweries') }}

),

final as (
    select
        *,
        -- Assuming 'fetched_at' exists in your raw table from the ingestion process
        -- If not, replace with CURRENT_TIMESTAMP()
        coalesce(fetched_at, current_timestamp()) as fetched_at_ts,
        date(coalesce(fetched_at, current_timestamp())) as fetched_date

    from source
),

renamed_and_cleaned as (

    select
        -- Use a BigQuery hash function as a surrogate key (no macro needed)
        -- FARM_FINGERPRINT is native BigQuery and creates a stable hash
        farm_fingerprint(cast(id as string) || fetched_date) as brewery_unique_key,
        
        -- Primary Key & Metadata
        cast(id as string) as brewery_pk,
        name as brewery_name,
        brewery_type,
        fetched_date,
        
        -- Address and Location Cleaning
        address_1 as street_address_1,
        nullif(address_2, '') as street_address_2,
        nullif(address_3, '') as street_address_3,
        
        city,
        state_province,
        country,
        
        trim(postal_code) as postal_code,
        
        -- Data Type Casting
        safe_cast(latitude as numeric) as latitude,
        safe_cast(longitude as numeric) as longitude,
        
        -- Contact Info
        phone,
        website_url,
        
        fetched_at_ts as extracted_at_ts

    from final
)

select * from renamed_and_cleaned
-- Deduplication step (assumes multiple rows for the same brewery_pk on the same date)
qualify row_number() over (partition by brewery_pk, fetched_date order by extracted_at_ts desc) = 1