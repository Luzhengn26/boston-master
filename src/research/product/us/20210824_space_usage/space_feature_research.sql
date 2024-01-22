-- Space Feature Research
-- by Henry Xu
-- August 2021








---------------------------------------------
--------- Full Funnel Mapping----------------
---------------------------------------------





--- Create temp tables to speed up queries 



-- us_space
select
	kec.user_id AS user_id,
	kig.country,
	kec.se_label,
	ket.se_action,
	kec.se_value,
	kec.se_property,
	kec.collector_tstamp
into temp table us_space
from etl_reporting.ksp_event_core kec 
inner join etl_reporting.ksp_event_types ket 
	on kec.event_type = ket.event_type 
inner join etl_reporting.ksp_ip_geo kig 
	on kec.event_id = kig.event_id 
where kig.country = 'US'
	and kec.collector_tstamp >= '2021-01-01'
;


-- step_1 select_space_step
with space_view as (
	select 
		u.user_id,
		u.se_action,
		u.collector_tstamp
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	where 1=1
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
),
select_view as (
	select 
		sv.se_action as step_1_1_action,
		u.se_label,
		u.se_action,
		u.user_id,
		u.collector_tstamp
	from space_view as sv
	inner join us_space as u 
		on sv.user_id = u.user_id
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > sv.collector_tstamp
		
)
select
	sev.se_label as step_1_label,
	sev.se_action as step_1_2_action,
	u.se_label,
	u.se_action,
	u.user_id,
	u.collector_tstamp
into temp table step_1
from us_space as u 
inner join select_view as sev
	on u.user_id = sev.user_id
	and sev.se_label = 'select_space_step'
	and sev.se_action = 'space_creation_view'
	and u.se_label = 'select_space_step'
	and u.se_action = 'space_creation_click'
	and u.collector_tstamp > sev.collector_tstamp
	and datediff(hour, sev.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
;





-- step_2 name_and_tnc_step

with name_view as (
	select
		s1.se_label as step_1_label,
		s1.se_action as step_1_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_1 as s1
		on u.user_id = s1.user_id
		and s1.se_label = 'select_space_step'
		and s1.se_action = 'space_creation_click'
		and u.se_label = 'name_and_tnc_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s1.collector_tstamp
		and datediff(hour, s1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	nv.se_label as step_2_label,
	nv.se_action as step_2_1_action,
	u.user_id,
	u.collector_tstamp,
	u.se_label,
	u.se_action
into temp table step_2
from us_space as u 
inner join name_view as nv
	on u.user_id = nv.user_id
	and nv.se_label = 'name_and_tnc_step'
	and nv.se_action = 'space_creation_view'
	and u.se_label = 'name_and_tnc_step'
	and u.se_action = 'space_creation_click'
	and u.collector_tstamp > nv.collector_tstamp 
	and datediff(hour, nv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
;



-- step_3 goal_step
-- table step_3 stops at creation view
-- table step_3_1 stops at goal_click
-- table step_3_2 steop at skip_clic

select
	s2.se_label as step_2_label,
	s2.se_action as step_2_2_action,
	u.se_label,
	u.se_action,
	u.se_value,
	u.user_id,
	u.collector_tstamp
into temp table step_3
from us_space as u 
inner join step_2 as s2
	on u.user_id = s2.user_id
	and s2.se_label = 'name_and_tnc_step'
	and s2.se_action = 'space_creation_click'
	and u.se_label = 'goal_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > s2.collector_tstamp 
	and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
;



-- table step_3_1 set_goal_amount



select 
	s3.se_label as step_3_label,
	s3.se_action as step_3_1_action,
	u.se_label,
	u.se_action,
	u.se_value,
	u.user_id,
	u.collector_tstamp
into temp table step_3_1
from us_space as u
inner join step_3 as s3
	on u.user_id = s3.user_id
	and s3.se_label = 'goal_step'
	and s3.se_action = 'space_creation_view'
	and u.se_label = 'goal_step'
	and u.se_action = 'set_goal_amount'
	and u.collector_tstamp > s3.collector_tstamp 
	and datediff(hour, s3.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
;






--- Table step_3_2 goal skip


select 
	s3.se_label as step_3_label,
	s3.se_action as step_3_2_action,
	u.se_label,
	u.se_action,
	u.user_id,
	u.collector_tstamp
into temp table step_3_2
from us_space as u 
inner join step_3 as s3
	on u.user_id = s3.user_id
	and s3.se_label = 'goal_step'
	and s3.se_action = 'space_creation_view'
	and u.se_label = 'goal_step'
	and u.se_action = 'skip_click'
	and u.collector_tstamp > s3.collector_tstamp 
	and datediff(hour, s3.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
;













--------------------------------------------------
-----------------Begin Analysis-------------------
--------------------------------------------------






--- spaces_viewed
--56107

		
select
	count(distinct du.user_id)
from us_space as u
inner join dbt.dim_users du 
	on du.shadow_user_id = u.user_id
where 1=1
	and du.kyc_first_completed <= '2021_03_31'
	and u.se_action = 'spaces_viewed'
	and u.collector_tstamp between '2021-01-01' and '2021-03-31'
;




--- spaces_viewed to select_space_step where space_creation_viewed
-- 9520


with space_view as (
	select 
		u.user_id,
		u.se_action,
		u.collector_tstamp
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	where 1=1
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and collector_tstamp between '2021-01-01' and '2021-03-31'
)
select 
	sv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id)
from space_view as sv
inner join us_space as u 
	on sv.user_id = u.user_id
	and u.se_label = 'select_space_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > sv.collector_tstamp
	and datediff(hour, sv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3
;






--- count space_creation_click
-- 3697

with space_view as (
	select 
		u.user_id,
		u.se_action,
		u.collector_tstamp
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	where 1=1
		and du.kyc_first_completed < '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
),
select_view as (
	select 
		sv.se_action as step_1_1_action,
		sv.collector_tstamp as pre_stamp,
		u.se_label,
		u.se_action,
		u.user_id,
		u.collector_tstamp
	from space_view as sv
	inner join us_space as u 
		on sv.user_id = u.user_id
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > sv.collector_tstamp 
		
)
select
	sev.se_label,
	sev.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join select_view as sev
	on u.user_id = sev.user_id
	and sev.se_label = 'select_space_step'
	and sev.se_action = 'space_creation_view'
	and u.se_label = 'select_space_step'
	and u.se_action = 'space_creation_click'
	and u.collector_tstamp > sev.collector_tstamp 
	and datediff(hour, sev.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;




--- count space creation click split by user's choice 
-- se_value = 0, Create now, decide lateral , 2219
-- se_value = 1, Saving for a goal, 2017

with space_view as (
	select 
		u.user_id,
		u.se_action,
		u.se_value,
		u.collector_tstamp
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	where 1=1
		and du.kyc_first_completed < '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
),
select_view as (
	select 
		sv.se_action as step_1_1_action,
		u.se_label,
		u.se_action,
		u.user_id,
		u.se_value,
		u.collector_tstamp
	from space_view as sv
	inner join us_space as u 
		on sv.user_id = u.user_id
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > sv.collector_tstamp 
		
)
select
	sev.se_label,
	sev.se_action,
	u.se_label,
	u.se_action,
	u.se_value,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join select_view as sev
	on u.user_id = sev.user_id
	and sev.se_label = 'select_space_step'
	and sev.se_action = 'space_creation_view'
	and u.se_label = 'select_space_step'
	and u.se_action = 'space_creation_click'
	and u.collector_tstamp > sev.collector_tstamp 
	and datediff(hour, sev.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4,5
;













--- Step 2 name_and_tnc_step


-- Count space creation view of name step
-- 3656
select
	s1.se_label,
	s1.se_action,
	u.se_label,
	u.se_action,
	u.se_value,
	count(distinct s1.user_id) as user_cnt
from us_space as u 
inner join step_1 as s1
	on u.user_id = s1.user_id
	and u.se_label = 'name_and_tnc_step'
	and u.se_action = 'space_creation_view'
	and s1.se_label = 'select_space_step'
	and s1.se_action = 'space_creation_click'
	and u.collector_tstamp > s1.collector_tstamp 
	and datediff(hour, s1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4,5
;

-- Count space creation click
-- 3086

with creation_view as (
	select
		s1.se_label as step_1_label,
		s1.se_action as step_1_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_1 as s1
		on u.user_id = s1.user_id
		and s1.se_label = 'select_space_step'
		and s1.se_action = 'space_creation_click'
		and u.se_label = 'name_and_tnc_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s1.collector_tstamp 
		and datediff(hour, s1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	cv.se_label,
	cv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join creation_view as cv
	on u.user_id = cv.user_id
	and cv.se_label = 'name_and_tnc_step'
	and cv.se_action = 'space_creation_view'
	and u.se_label = 'name_and_tnc_step'
	and u.se_action = 'space_creation_click'
	and u.collector_tstamp > cv.collector_tstamp 
	and datediff(hour, cv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







--- Step 3 goal_step

-- Count space_creation_view
-- 1616

select
	s2.se_label,
	s2.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join step_2 as s2
	on u.user_id = s2.user_id
	and s2.se_label = 'name_and_tnc_step'
	and s2.se_action = 'space_creation_click'
	and u.se_label = 'goal_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > s2.collector_tstamp 
	and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;


-- count set goal amount 
-- 1009

with goal_view as (
	select
		s2.se_label as step_2_label,
		s2.se_action as step_2_2_action,
		u.se_label,
		u.se_action,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_2 as s2
		on u.user_id = s2.user_id
		and s2.se_label = 'name_and_tnc_step'
		and s2.se_action = 'space_creation_click'
		and u.se_label = 'goal_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s2.collector_tstamp 
		and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	gv.se_label,
	gv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join goal_view as gv
	on u.user_id = gv.user_id
	and gv.se_label = 'goal_step'
	and gv.se_action = 'space_creation_view'
	and u.se_label = 'goal_step'
	and u.se_action = 'set_goal_amount'
	and u.collector_tstamp > gv.collector_tstamp 
	and datediff(hour, gv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;





-- Count goal click
-- 132, potentially misfire 


with goal_view as (
	select
		s2.se_label as step_2_label,
		s2.se_action as step_2_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_2 as s2
		on u.user_id = s2.user_id
		and s2.se_label = 'name_and_tnc_step'
		and s2.se_action = 'space_creation_click'
		and u.se_label = 'goal_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s2.collector_tstamp 
		and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
),
set_goal as (
	select 
		gv.se_label as step_3_label,
		gv.se_action as step_3_1_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join goal_view as gv 
		on u.user_id = gv.user_id
		and gv.se_label = 'goal_step'
		and gv.se_action = 'space_creation_view'
		and u.se_label = 'goal_step'
		and u.se_action = 'set_goal_amount'
		and u.collector_tstamp > gv.collector_tstamp 
		and datediff(hour, gv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	sg.se_label,
	sg.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join set_goal as sg
	on u.user_id = sg.user_id
	and sg.se_label = 'goal_step'
	and sg.se_action = 'set_goal_amount'
	and u.se_label = 'goal_step'
	and u.se_action = 'goal_click'
	and u.collector_tstamp > sg.collector_tstamp 
	and datediff(hour, sg.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;









-- Count skip click of goal step
-- all skip_clicks
-- 553

with goal_view as (
	select
		s2.se_label as step_2_label,
		s2.se_action as step_2_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_2 as s2
		on u.user_id = s2.user_id
		and s2.se_label = 'name_and_tnc_step'
		and s2.se_action = 'space_creation_click'
		and u.se_label = 'goal_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s2.collector_tstamp 
		and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	gv.se_label,
	gv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join goal_view as gv 
	on u.user_id = gv.user_id
	and gv.se_label = 'goal_step'
	and gv.se_action = 'space_creation_view'
	and u.se_label = 'goal_step'
	and u.se_action = 'skip_click'
	and u.collector_tstamp > gv.collector_tstamp 
	and datediff(hour, gv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;





-- Count cancel click from goal step
-- all cancels
-- 116


with goal_view as (
	select
		s2.se_label as step_2_label,
		s2.se_action as step_2_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_2 as s2
		on u.user_id = s2.user_id
		and s2.se_label = 'name_and_tnc_step'
		and s2.se_action = 'space_creation_click'
		and u.se_label = 'goal_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s2.collector_tstamp 
		and datediff(hour, s2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	gv.se_label,
	gv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join goal_view as gv
	on u.user_id = gv.user_id
	and gv.se_label = 'goal_step'
	and gv.se_action = 'space_creation_view'
	and u.se_label = 'goal_step'
	and u.se_action = 'cancel_click'
	and u.collector_tstamp > gv.collector_tstamp 
	and datediff(hour, gv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







--- Step 4 transfer_step

-- Count space create view of transfer step from step_3_1 (set goal)
-- 981

select
	s3_1.se_label,
	s3_1.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join step_3_1 as s3_1
	on u.user_id = s3_1.user_id
	and s3_1.se_label = 'goal_step'
	and s3_1.se_action = 'set_goal_amount'
	and u.se_label = 'transfer_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > s3_1.collector_tstamp 
	and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;








-- Count space create view of transfer step from step_3_2 (skip)
-- 544


select
	s3_2.se_label,
	s3_2.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join step_3_2 as s3_2
	on u.user_id = s3_2.user_id
	and s3_2.se_label = 'goal_step'
	and s3_2.se_action = 'skip_click'
	and u.se_label = 'transfer_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > s3_2.collector_tstamp 
	and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;






-- count transfer_amount of transfer step from step_3_1 (set goal)
-- 385


with transfer_view as (
	select
		s3_1.se_label as step_3_label,
		s3_1.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_1 as s3_1
		on u.user_id = s3_1.user_id
		and s3_1.se_label = 'goal_step'
		and s3_1.se_action = 'set_goal_amount'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_1.collector_tstamp 
		and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv 
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'transfer_amount'
	and u.collector_tstamp > tv.collector_tstamp 
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;






-- count transfer_click from step_3_1 (set goal)
-- 36, potential event misfire

with transfer_view as (
	select
		s3_1.se_label as step_3_label,
		s3_1.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_1 as s3_1
		on u.user_id = s3_1.user_id
		and s3_1.se_label = 'goal_step'
		and s3_1.se_action = 'set_goal_amount'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_1.collector_tstamp 
		and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
),
transfer_amt as (
	select 
		tv.se_label as step_4_label,
		tv.se_action as step_4_1_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join transfer_view as tv
		on u.user_id = tv.user_id
		and tv.se_label = 'transfer_step'
		and tv.se_action = 'space_creation_view'
		and u.se_label = 'transfer_step'
		and u.se_action = 'transfer_amount'
		and u.collector_tstamp > tv.collector_tstamp 
		and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	ta.se_label,
	ta.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_amt as ta 
	on u.user_id = ta.user_id
	and ta.se_label = 'transfer_step'
	and ta.se_action = 'transfer_amount'
	and u.se_label = 'transfer_step'
	and u.se_action = 'transfer_click'
	and u.collector_tstamp > ta.collector_tstamp 
	and datediff(hour, ta.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;




-- Count skip_click of transfer step from step_3_1 (set goal)
-- 598

with transfer_view as (
	select
		s3_1.se_label as step_3_label,
		s3_1.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_1 as s3_1
		on u.user_id = s3_1.user_id
		and s3_1.se_label = 'goal_step'
		and s3_1.se_action = 'set_goal_amount'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_1.collector_tstamp 
		and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'skip_click'
	and u.collector_tstamp > tv.collector_tstamp 
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







-- Count back_click of transfer step from step_3_1 (set goal)
-- 92

with transfer_view as (
	select
		s3_1.se_label as step_3_label,
		s3_1.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_1 as s3_1
		on u.user_id = s3_1.user_id
		and s3_1.se_label = 'goal_step'
		and s3_1.se_action = 'set_goal_amount'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_1.collector_tstamp
		and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv 
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'back_click'
	and u.collector_tstamp > tv.collector_tstamp
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







-- count transfer_amount of transfer step from step_3_2 (skip)
-- 199 

with transfer_view as (
	select
		s3_2.se_label as step_3_label,
		s3_2.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_2 as s3_2
		on u.user_id = s3_2.user_id
		and s3_2.se_label = 'goal_step'
		and s3_2.se_action = 'skip_click'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_2.collector_tstamp
		and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv 
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'transfer_amount'
	and u.collector_tstamp > tv.collector_tstamp
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;






-- count transfer_click from step_3_2 (skip)
-- 15, potential misfire

with transfer_view as (
	select
		s3_2.se_label as step_3_label,
		s3_2.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_2 as s3_2
		on u.user_id = s3_2.user_id
		and s3_2.se_label = 'goal_step'
		and s3_2.se_action = 'skip_click'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_2.collector_tstamp
		and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
),
transfer_amt as (
	select 
		tv.se_label as step_4_label,
		tv.se_action as step_4_1_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join transfer_view as tv
		on u.user_id = tv.user_id
		and tv.se_label = 'transfer_step'
		and tv.se_action = 'space_creation_view'
		and u.se_label = 'transfer_step'
		and u.se_action = 'transfer_amount'
		and u.collector_tstamp > tv.collector_tstamp
		and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	ta.se_label,
	ta.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_amt as ta 
	on u.user_id = ta.user_id
	and ta.se_label = 'transfer_step'
	and ta.se_action = 'transfer_amount'
	and u.se_label = 'transfer_step'
	and u.se_action = 'transfer_click'
	and u.collector_tstamp > ta.collector_tstamp
	and datediff(hour, ta.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







-- Count skip_click from step_3_2 (skip)
-- 332

with transfer_view as (
	select
		s3_2.se_label as step_3_label,
		s3_2.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_2 as s3_2
		on u.user_id = s3_2.user_id
		and s3_2.se_label = 'goal_step'
		and s3_2.se_action = 'skip_click'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_2.collector_tstamp
		and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv 
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'skip_click'
	and u.collector_tstamp > tv.collector_tstamp
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;





-- Count back_click from step_3_2 (skip)
-- 69

with transfer_view as (
	select
		s3_2.se_label as step_3_label,
		s3_2.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_2 as s3_2
		on u.user_id = s3_2.user_id
		and s3_2.se_label = 'goal_step'
		and s3_2.se_action = 'skip_click'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_2.collector_tstamp
		and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	tv.se_label,
	tv.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_view as tv
	on u.user_id = tv.user_id
	and tv.se_label = 'transfer_step'
	and tv.se_action = 'space_creation_view'
	and u.se_label = 'transfer_step'
	and u.se_action = 'back_click'
	and u.collector_tstamp > tv.collector_tstamp
	and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;







--- Step 5 round_up_step
--- Only two calculations are conducted in this section as roundup_step is tracked by another team 
--- and not the priority for this research related to Space 
--- The reason for running the two calculation is that there were potential missed firing of events of transfer_click
--- and these two calculation helps make an estimate of the count of users triggering transfer_click 




-- count space_creation_view from step_3_1 transfer
-- 329

with transfer_view as (
	select
		s3_1.se_label as step_3_label,
		s3_1.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_1 as s3_1
		on u.user_id = s3_1.user_id
		and s3_1.se_label = 'goal_step'
		and s3_1.se_action = 'set_goal_amount'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_1.collector_tstamp
		and datediff(hour, s3_1.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
),
transfer_amt as (
	select 
		tv.se_label as step_4_label,
		tv.se_action as step_4_1_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join transfer_view as tv
		on u.user_id = tv.user_id
		and tv.se_label = 'transfer_step'
		and tv.se_action = 'space_creation_view'
		and u.se_label = 'transfer_step'
		and u.se_action = 'transfer_amount'
		and u.collector_tstamp > tv.collector_tstamp
		and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	ta.se_label,
	ta.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_amt as ta
	on u.user_id = ta.user_id
	and ta.se_label = 'transfer_step'
	and ta.se_action = 'transfer_amount'
	and u.se_label = 'roundup_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > ta.collector_tstamp
	and datediff(hour, ta.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;



-- count roundup view from step_3_2 (transfer amount)

with transfer_view as (
	select
		s3_2.se_label as step_3_label,
		s3_2.se_action as step_3_2_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join step_3_2 as s3_2
		on u.user_id = s3_2.user_id
		and s3_2.se_label = 'goal_step'
		and s3_2.se_action = 'skip_click'
		and u.se_label = 'transfer_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > s3_2.collector_tstamp
		and datediff(hour, s3_2.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
),
transfer_amt as (
	select 
		tv.se_label as step_4_label,
		tv.se_action as step_4_1_action,
		u.se_label,
		u.se_action,
		u.se_value,
		u.user_id,
		u.collector_tstamp
	from us_space as u 
	inner join transfer_view as tv
		on u.user_id = tv.user_id
		and tv.se_label = 'transfer_step'
		and tv.se_action = 'space_creation_view'
		and u.se_label = 'transfer_step'
		and u.se_action = 'transfer_amount'
		and u.collector_tstamp > tv.collector_tstamp
		and datediff(hour, tv.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
)
select 
	ta.se_label,
	ta.se_action,
	u.se_label,
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join transfer_amt as ta 
	on u.user_id = ta.user_id
	and ta.se_label = 'transfer_step'
	and ta.se_action = 'transfer_amount'
	and u.se_label = 'roundup_step'
	and u.se_action = 'space_creation_view'
	and u.collector_tstamp > ta.collector_tstamp
	and datediff(hour, ta.collector_tstamp::timetz, u.collector_tstamp::timetz) < 24
group by 1,2,3,4
;




----------------------------------
----------------------------------
----------------------------------
--- Count Space account actually created 
-- 1895


select
	se_label,
	se_action,
	da.account_role,
	count(distinct s2.user_id) as user_cnt
from step_2 as s2
inner join dbt.dim_users du 
	on du.shadow_user_id = s2.user_id
inner join dbt.dim_accounts da 
	on du.user_id  = da.user_id 
where 1=1
	and s2.se_label = 'name_and_tnc_step'
	and s2.se_action = 'space_creation_click'
	and da.account_role  = 'SECONDARY'
	and da.opened_at > s2.collector_tstamp
group by 1,2,3
;



--- Count how many users actually funded these Spaces
-- 2174

with funding as (
	select
		s2.se_label,
		s2.se_action,
		da.user_id,
		da.account_id,
		count(ft.txn_id) as txn_cnt
	from step_2 as s2
	inner join dbt.dim_users du 
		on du.shadow_user_id = s2.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
	inner join dbt.f_transactions ft 
		on ft.account_id = da.account_id
	where 1=1
		and s2.se_label = 'name_and_tnc_step'
		and s2.se_action = 'space_creation_click'
		and da.account_role  = 'SECONDARY'
		and da.opened_at > s2.collector_tstamp
	group by 1,2,3,4
	having count(ft.txn_id) > 0
)
select
	count(distinct f.user_id) as user_funded_cnt
from funding as f
;







----------------------------------------
----------------------------------------
---Upper Funnel Analysis----------------
----------------------------------------
----------------------------------------




---- Number of users who at the moment they viewed space tab, have
-- 1 space open 16078
-- 2 spaces open 8381


with space_open as (
	select
		da.user_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so.user_id)
from space_open as so
where acct_cnt = 2 -- change this to 1 for 1 space
;
	
	



--- count users who at the moment they viewed space tab, have 0 space open
--- based on the assumption that a user has to have at least one main account open 
-- 38679

with space_open as (
	select
		da.user_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so.user_id) as user_cnt
from space_open as so
where acct_cnt = 1
;








-------------------------------------------------------
-------------------------------------------------------
-----Counting with no need of assumption --------------
-------------------------------------------------------


-- We can't always assume that a user performs the action within a certain period of viewing the tab 
-- and that his/her count of spaces has not changed
-- So as an alternative way:

--- Out of the users who have 0 space at the time space tab is viewed
-- when they click create a space
-- how many users still have 0 space
-- 6840




with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 1
),
select_view as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as select_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sv.da_id,
		sv.select_stamp,
		count(distinct da.account_id) as acct_cnt
	from select_view as sv
	inner join dbt.dim_accounts da 
		on sv.da_id  = da.user_id 
		and da.opened_at < sv.select_stamp
		and (da.closed_at > sv.select_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so2.da_id) as user_cnt
from space_open_2 as so2
where acct_cnt =1
;





--- Out of the users who have 1 space at the time space tab is viewed
-- when they click create a space
-- how many users still have 1 space
-- 2160



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 1
),
select_view as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as select_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sv.da_id,
		sv.select_stamp,
		count(distinct da.account_id) as acct_cnt
	from select_view as sv
	inner join dbt.dim_accounts da 
		on sv.da_id  = da.user_id 
		and da.opened_at < sv.select_stamp
		and (da.closed_at > sv.select_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct da_id) as user_cnt
from space_open_2 as so2
where acct_cnt =2
;


	


--- Out of the users who have 1 space at the time space tab is viewed
-- when they drag and drop
-- how many users still have 1 space
-- 4430



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 1
),
dad_view as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as dad_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_action = 'spaces.dadmovemoney_viewed'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		dad.da_id,
		dad.dad_stamp,
		count(distinct da.account_id) as acct_cnt
	from dad_view as dad
	inner join dbt.dim_accounts da 
		on dad.da_id  = da.user_id 
		and da.opened_at < dad.dad_stamp
		and (da.closed_at > dad.dad_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct da_id) as user_cnt
from space_open_2 as so2
where acct_cnt =2
;


	


--- Out of the users who have 1 space at the time space tab is viewed
-- when they click on an exisitng space
-- how many users still have 1 space
-- 9696



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 1
),
space_click as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as click_space_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_action = 'spaces.insidespace_viewed'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sc.da_id,
		sc.click_space_stamp,
		count(distinct da.account_id) as acct_cnt
	from space_click as sc
	inner join dbt.dim_accounts da 
		on sc.da_id  = da.user_id 
		and da.opened_at < sc.click_space_stamp
		and (da.closed_at > sc.click_space_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so2.da_id) as user_cnt
from space_open_2 as so2
where acct_cnt =2
;






--- Out of the users who have 2 spaces at the time space tab is viewed
-- when they click create a space
-- how many users still have 2 spaces
-- 1600



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 2
),
space_click as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as select_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sc.da_id,
		sc.select_stamp,
		count(distinct da.account_id) as acct_cnt
	from space_click as sc
	inner join dbt.dim_accounts da 
		on sc.da_id  = da.user_id 
		and da.opened_at < sc.select_stamp
		and (da.closed_at > sc.select_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so2.da_id) as user_cnt
from space_open_2 as so2
where acct_cnt = 3
;







--- Out of the users who have 2 spaces at the time space tab is viewed
-- when they drag and drop
-- how many users still have 2 spaces
-- 3887



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 2
),
dad_view as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as dad_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_action = 'spaces.dadmovemoney_viewed'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		dad.da_id,
		dad.dad_stamp,
		count(distinct da.account_id) as acct_cnt
	from dad_view as dad
	inner join dbt.dim_accounts da 
		on dad.da_id  = da.user_id 
		and da.opened_at < dad.dad_stamp
		and (da.closed_at > dad.dad_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so2.da_id) as user_cnt
from space_open_2 as so2
where acct_cnt = 3
;




--- Out of the users who have 2 spaces at the time space tab is viewed
-- when they click on an exisitng space
-- how many users still have 2 spaces
-- 6346



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 2
),
space_click as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as click_space_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_action = 'spaces.insidespace_viewed'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sc.da_id,
		sc.click_space_stamp,
		count(distinct da.account_id) as acct_cnt
	from space_click as sc
	inner join dbt.dim_accounts da 
		on sc.da_id  = da.user_id 
		and da.opened_at < sc.click_space_stamp
		and (da.closed_at > sc.click_space_stamp or da.closed_at is null)
	group by 1,2
)
select
	count(distinct so2.da_id) as user_cnt
from space_open_2 as so2
where acct_cnt = 3
;









---------------------------------------------
---------------------------------------------
---------------3rd Space---------------------
---------------------------------------------
---------------------------------------------





--Not tracked:
--View_space_type_bottom_sheet_limit_reached 490
--Spaces.spacelimit_viewed 1104
--
--Tracked:
--View_space_type_bottom_sheet_no_available_space 737
--Click_space_type_bottom_sheet_no_available_space_dismiss 593
--
--
--Count of people who triggered both - 157
--View_space_type_bottom_sheet_limit_reached
--View_space_type_bottom_sheet_no_available_space
--
--Count of people who triggered both - 237
--Spaces.spacelimit_viewed
--View_space_type_bottom_sheet_limit_reached
--
--Count of people who triggered both - 364
--Spaces.spacelimit_viewed
--View_space_type_bottom_sheet_no_available_space



--- Explore 

select
	count(distinct user_id)
from us_space u
where se_action = 'view_space_type_bottom_sheet_limit_reached'
	and collector_tstamp between '2021-01-01' and '2021-03-31'
;




select
	count(distinct user_id)
from us_space u
where se_action = 'spaces.spacelimit_viewed'
	and collector_tstamp between '2021-01-01' and '2021-03-31'
;



select
	count(distinct user_id)
from us_space u
where se_action = 'view_space_type_bottom_sheet_no_available_space'
	and collector_tstamp between '2021-01-01' and '2021-03-31'
;


select
	count(distinct user_id)
from us_space u
where se_action = 'click_space_type_bottom_sheet_no_available_space_dismiss'
	and collector_tstamp between '2021-01-01' and '2021-03-31'
;




select
	count(distinct u1.user_id)
from us_space u1
inner join us_space u2
 	on u1.user_id = u2.user_id 
	and u1.se_action = 'view_space_type_bottom_sheet_limit_reached'
	and u2.se_action = 'view_space_type_bottom_sheet_no_available_space'
	and u1.collector_tstamp between '2021-01-01' and '2021-03-31'
;



select
	count(distinct u1.user_id)
from us_space u1
inner join us_space u2
 	on u1.user_id = u2.user_id 
	and u1.se_action = 'view_space_type_bottom_sheet_limit_reached'
	and u2.se_action = 'spaces.spacelimit_viewed'
	and u1.collector_tstamp between '2021-01-01' and '2021-03-31'
;




select
	count(distinct u1.user_id)
from us_space u1
inner join us_space u2
 	on u1.user_id = u2.user_id 
	and u1.se_action = 'view_space_type_bottom_sheet_no_available_space'
	and u2.se_action = 'spaces.spacelimit_viewed'
	and u1.collector_tstamp between '2021-01-01' and '2021-03-31'
;






--- Out of the users who have 2 spaces at the time space tab is viewed
-- when they click create a space
-- how many users still have 2 spaces
-- 1600
-- and how many run into view_space_type_bottom_sheet_no_available_space - 951
-- how many run into view_space_type_bottom_sheet_limit_reached - 405
-- how many run into spaces.spacelimit_viewed - 32



with space_open_1 as (
	select
		u.user_id as u_id,
		da.user_id as da_id,
		u.collector_tstamp,
		count(distinct da.account_id) as acct_cnt
	from us_space as u
	inner join dbt.dim_users du 
		on du.shadow_user_id = u.user_id
	inner join dbt.dim_accounts da 
		on du.user_id  = da.user_id 
		and du.kyc_first_completed <= '2021_03_31'
		and u.se_action = 'spaces_viewed'
		and u.collector_tstamp between '2021-01-01' and '2021-03-31'
		and da.account_role = 'SECONDARY'
		and da.opened_at < u.collector_tstamp 
		and (da.closed_at > u.collector_tstamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 2
),
select_view as (
	select
		so1.u_id,
		so1.da_id,
		so1.collector_tstamp as view_stamp,
		u.se_label,
		u.se_action,
		u.collector_tstamp as select_stamp
	from space_open_1 as so1
	inner join us_space u
		on so1.u_id = u.user_id 
		and u.se_label = 'select_space_step'
		and u.se_action = 'space_creation_view'
		and u.collector_tstamp > so1.collector_tstamp
),
space_open_2 as (
	select
		sv.da_id,
		sv.u_id,
		sv.select_stamp,
		count(distinct da.account_id) as acct_cnt
	from select_view as sv
	inner join dbt.dim_accounts da 
		on sv.da_id  = da.user_id 
		and da.opened_at < sv.select_stamp
		and (da.closed_at > sv.select_stamp or da.closed_at is null)
	group by 1,2,3
	having count(distinct da.account_id) = 3
)
select 
	u.se_action,
	count(distinct u.user_id) as user_cnt
from us_space as u 
inner join space_open_2 as so2
	on u.user_id = so2.u_id
	and u.se_action = 'view_space_type_bottom_sheet_no_available_space' -- change this to different error message action to get different count
	and u.collector_tstamp > so2.select_stamp
group by 1
;





