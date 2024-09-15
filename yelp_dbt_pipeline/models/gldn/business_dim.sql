{{ config(materialized='table') }}

select 
    business_id,
    initcap(trim(name)) as name,
    initcap(trim(address)) as address,
    initcap(trim(city)) as city,
    upper(state) as state,
    latitude,
    longitude,
    trim(postal_code) as postal_code,
    cast(is_open as boolean) as is_open,
    stars,
    cast(review_count as integer) as review_count,
    cast(to_char(to_timestamp(monday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as monday_opening_time,
    cast(to_char(to_timestamp(monday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as monday_closing_time,
    cast(to_char(to_timestamp(tuesday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as tuesday_opening_time,
    cast(to_char(to_timestamp(tuesday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as tuesday_closing_time,
    cast(to_char(to_timestamp(wednesday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as wednesday_opening_time,
    cast(to_char(to_timestamp(wednesday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as wednesday_closing_time,
    cast(to_char(to_timestamp(thursday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as thursday_opening_time,
    cast(to_char(to_timestamp(thursday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as thursday_closing_time,
    cast(to_char(to_timestamp(friday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as friday_opening_time,
    cast(to_char(to_timestamp(friday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as friday_closing_time,
    cast(to_char(to_timestamp(saturday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as saturday_opening_time,
    cast(to_char(to_timestamp(saturday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as saturday_closing_time,
    cast(to_char(to_timestamp(sunday_opening_time, 'HH24:MI'), 'HH24:MI') as time) as sunday_opening_time,
    cast(to_char(to_timestamp(sunday_closing_time, 'HH24:MI'), 'HH24:MI') as time) as sunday_closing_time,
    cast(
        case 
            when trim(businessacceptscreditcards) = 'true' then true
            when trim(businessacceptscreditcards) = 'false' then false
            else null end
        as boolean) as businessacceptscreditcards,
    cast(restaurantspricerange2 as integer) restaurantspricerange,
    cast(
        case 
            when trim(restaurantsdelivery) = 'true' then true
            when trim(restaurantsdelivery) = 'false' then false
            else null end
        as boolean) as restaurantsdelivery,
    cast(
        case 
            when trim(restaurantstakeout) = 'true' then true
            when trim(restaurantstakeout) = 'false' then false
            else null end
        as boolean) as restaurantstakeout,
    cast(
        case 
            when trim(bikeparking) = 'true' then true
            when trim(bikeparking) = 'false' then false
            else null end
        as boolean) as bikeparking,
    cast(
        case 
            when trim(byappointmentonly) = 'true' then true
            when trim(byappointmentonly) = 'false' then false
            else null end
        as boolean) as byappointmentonly,
    cast(
        case 
            when trim(caters) = 'true' then true
            when trim(caters) = 'false' then false
            else null end
        as boolean) as caters,
    cast(
        case 
            when trim(restaurantsreservations) = 'true' then true
            when trim(restaurantsreservations) = 'false' then false
            else null end
        as boolean) as restaurants_reservations,
    cast(
        case 
            when trim(outdoorseating) = 'true' then true
            when trim(outdoorseating) = 'false' then false
            else null end
        as boolean) as outdoor_seating,
    cast(
        case 
            when trim(hastv) = 'true' then true
            when trim(hastv) = 'false' then false
            else null end
        as boolean) as has_tv,
    cast(
        case 
            when trim(goodforkids) = 'true' then true
            when trim(goodforkids) = 'false' then false
            else null end
        as boolean) as good_for_kids,
    cast(
        case 
            when trim(restaurantsgoodforgroups) = 'true' then true
            when trim(restaurantsgoodforgroups) = 'false' then false
            else null end
        as boolean) as restaurantsgoodforgroups,
    cast(
        case 
            when trim(businessparking_garage) = 'true' then true
            when trim(businessparking_garage) = 'false' then false
            else null end
        as boolean) as parking_garage,
    cast(
        case 
            when trim(businessparking_lot) = 'true' then true
            when trim(businessparking_lot) = 'false' then false
            else null end
        as boolean) as parking_lot,
    cast(
        case 
            when trim(businessparking_street) = 'true' then true
            when trim(businessparking_street) = 'false' then false
            else null end
        as boolean) as parking_street,
    cast(
        case 
            when trim(businessparking_valet) = 'true' then true
            when trim(businessparking_valet) = 'false' then false
            else null end
        as boolean) as parking_valet,
    cast(
        case 
            when trim(businessparking_validated) = 'true' then true
            when trim(businessparking_validated) = 'false' then false
            else null end
        as boolean) as parking_validated
from {{ source("stg","business") }}