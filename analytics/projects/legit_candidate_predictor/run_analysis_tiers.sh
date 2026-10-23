#!/usr/bin/env bash
# Analysis-tier variants for the v2 composition (86ak0je29).
#
# One run per claim, masking every column that would let a claim about that concept restate
# the scoring rule, then a second run adding the L2 easy-match family as substitution basis.
# The substitution is NOT assumed to work: each swap is measured, which is the whole point
# (SESSION_NOTES 2026-08-12, redundant families are substitution assets).
#
# Reference for every delta here is the SERVING composition (0.8824 / 0.8369 / 0.8924),
# not the pass-B baseline the runner prints against.
set -euo pipefail
cd "$(dirname "$0")/../.."

FAMILIES="br_position gp_user_details incumbency"

for claim in incumbency office_level district_size selfreport_effort; do
  echo "############ ${claim} : masked ############"
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
    projects/legit_candidate_predictor/exp_family.py \
    --family ${FAMILIES} --claim "${claim}" --manifest --ledger \
    --name "v2 ANALYSIS tier: no ${claim}" \
    --notes "86ak0je29 analysis tier. Claim protected: ${claim}. Compare to the v2 SERVING row (0.8824/0.8369/0.8924), not to the pass-B baseline."

  echo "############ ${claim} : masked + L2 substitution ############"
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
    projects/legit_candidate_predictor/exp_family.py \
    --family ${FAMILIES} --claim "${claim}" --substitute l2_easy --manifest --ledger \
    --name "v2 ANALYSIS tier: no ${claim} + L2 substitution" \
    --notes "86ak0je29 analysis tier with l2_easy added as substitution basis for the masked ${claim} columns. Compare to the matching masked-only row to read whether L2 backfills the discrimination."
done
echo "ALL ANALYSIS TIER RUNS COMPLETE"
