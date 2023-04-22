


with cluster_stats as (
	select 
		cid,
		year,
		intensity_25km as intensity, 
	case 
		when intensity_25km > 0 
		then 1 
		else 0 
	end conflict_dummy
from {{ ref('cluster_year_stats_all_with_lead') }}
),

groupings as(
select
	cid, year, intensity, conflict_dummy,
	sum(change_flag) over (partition by cid order by year) as grp
from (
	select
		*,
		case
			when conflict_dummy = lag(conflict_dummy) 
				over (partition by cid order by year)
			then 0
			else 1
		end change_flag
	from cluster_stats
)s
),

grouped_counts as (
select
	*,
	row_number() over (partition by cid, grp order by year) as cumulative_count
from groupings
)

select
	cid,
	year,
	intensity as intensity_25km,
	conflict_dummy as conflict_dummy_25km,
	grp,
	case
		when conflict_dummy = 0
		then 0
		else cumulative_count
	end cumulative_conflict_years_25km
from grouped_counts