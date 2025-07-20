with trial_goals as (
	select * from {{ ref('trial_goals') }}
)

select
	organization_id,
	is_shifts_approved_goal_met,
	goal_shifts_approved_met_timestamp,
	is_template_apply_modal_applied_goal_met,
	goal_template_apply_modal_applied_timestamp,
	is_active_days_7d_goal_met,
	goal_nb_active_days_7d_timestamp,
	is_shifts_approved_goal_met
		and is_template_apply_modal_applied_goal_met
		and is_active_days_7d_goal_met as is_trial_activated,
	case
		when is_trial_activated then greatest(
			goal_shifts_approved_met_timestamp,
			goal_template_apply_modal_applied_timestamp,
			goal_nb_active_days_7d_timestamp
		)
  		else null
	end as trial_activation_timestamp
from trial_goals
