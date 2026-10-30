select user_id, feature, is_user_action, first_used_at, last_used_at, use_days
from {{ ref("int__user_feature_use") }}
