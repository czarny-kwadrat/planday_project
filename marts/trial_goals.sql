with trial_activities as (
	select distinct
		organization_id,
		activity_name,
		timestamp as activity_timestamp,
		converted,
		converted_at,
		trial_start,
		trial_end
	from {{ ref('activity_data') }}
	where timestamp <= trial_end
),

all_organizations as (
	select distinct
		organization_id,
		trial_start,
		trial_end
	from trial_activities
),

numbered_activities AS (
    	select
        	organization_id,
		trial_start_date,
        	activity_name,
        	activity_timestamp,
		date_trunc('day', activity_timestamp) as activity_date,
        	row_number() over (
            		partition by organization_id, activity_name 
            		order by activity_timestamp
        	) as activity_order,
		dense_rank() over (
            		partition by organization_id 
            		order by activity_date
        	) as active_day_order
    	from trial_activities
),

goal_nb_shifts_approved as (
	select
		organization_id,
		-- Goal 1: Count of 'Scheduling.Shift.Approved' > 4
        	max(activity_order) as nb_shifts_approved,
		min(case
			when activity_order > 4 then activity_timestamp
			else null
		end) as goal_shifts_approved_met_timestamp
	from numbered_activities
	where activity_name = 'Scheduling.Shift.Approved'
	group by 1),

goal_template_apply_modal_applied as (
	select
		organization_id,
        	-- Goal 2: Count of 'Scheduling.Template.ApplyModal.Applied' > 2
        	max(activity_order) as nb_template_apply_modal_applied,
		min(case
			when activity_order > 2 then activity_timestamp
			else null
		end) as goal_template_apply_modal_applied_timestamp
	from numbered_activities
	where activity_name = 'Scheduling.Template.ApplyModal.Applied'
	group by 1
),

goal_active_days as (
	select
		organization_id,
        	-- Goal 3: Number of active days in first 7 days > 2
		count(distinct activity_date) as nb_active_days_7d,
		min(case
			when active_day_order = 3 then activity_timestamp
			else null
		end) as goal_nb_active_days_7d_timestamp
		
    	from numbered_activities
	where activity_timestamp < trial_start_date + interval '7 days'
	group by 1
)

select
	all_organizations.organization_id,
	goal_nb_shifts_approved.nb_shifts_approved,
	goal_nb_shifts_approved.nb_shifts_approved > 4 as is_shifts_approved_goal_met,
	goal_nb_shifts_approved.goal_shifts_approved_met_timestamp,
	goal_template_apply_modal_applied.nb_template_apply_modal_applied,
	goal_template_apply_modal_applied.nb_template_apply_modal_applied > 2 as is_template_apply_modal_applied_goal_met,
	goal_template_apply_modal_applied.goal_template_apply_modal_applied_timestamp,
	goal_active_days.nb_active_days_7d,
	goal_active_days.nb_active_days_7d > 2 as is_active_days_7d_goal_met,
	goal_active_days.goal_nb_active_days_7d_timestamp

from all_organizations
	left goal_nb_shifts_approved on all_organizations.organization_id = goal_nb_shifts_approved.organization_id
	left goal_template_apply_modal_applied on all_organizations.organization_id = goal_template_apply_modal_applied.organization_id
	left goal_active_days on all_organizations.organization_id = goal_active_days.organization_id

	