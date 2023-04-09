


with svac as (
    select year, actorid, conflictid,
        gwnoloc, gwnoloc2, state_prev,
        ai_prev, hrw_prev,
        case
            when child_prev > 0 then 1
            else 0
        end child_prev
    from {{ source('raw', 'svac') }}
),

actors as (
    select "ActorId" as actorid, "Org" as org
    from {{ source('raw', 'ucdp_actor') }}
),

svac_actors as (
    select 
        svac.*,
        case
            when actors.org < 4
            then 1
            else 0
        end state
    from (
        svac
        left join actors
        on svac.actorid = actors.actorid
    )
),

svac_clean as (
    select
        svac_actors.*,
        case
            when state_prev > 0
                or ai_prev > 0
                or hrw_prev > 0
            then 1
            else 0
        end adult_prev_minor,
        case
            when state_prev > 1
                or ai_prev > 1
                or hrw_prev > 1
            then 1
            else 0
        end adult_prev_major        
    from
        svac_actors
)

select * from svac_clean