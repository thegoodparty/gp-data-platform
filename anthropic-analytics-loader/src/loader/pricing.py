"""Published Anthropic pricing figures used as sanity ceilings on ingested cost data.

Not a pricing calculator -- we don't know each row's discount, batch/fast-mode/cache status, or
inference_geo multiplier, so we can't reproduce Anthropic's bill from these numbers. We only use
this as an upper bound: no single token, under any modifier, can cost more than the most expensive
published *standard* output-token rate, so amount/tokens for any bucket must stay under it.
"""

from __future__ import annotations

from decimal import Decimal

# Highest published standard output-token price across models in
# https://platform.claude.com/docs/en/about-claude/pricing#model-pricing (checked 2026-09-11):
# Claude Fable 5 and Fable 5.1 output tokens are $50/MTok, the highest of any model on that page
# (Fast mode on Opus 5/4.8 also caps out at $50/MTok, so this figure covers it too). Batch
# discounts only lower price, so the standard rate is already the ceiling.
MAX_PUBLISHED_OUTPUT_PRICE_PER_MILLION_TOKENS = Decimal("50.00")

MAX_PUBLISHED_OUTPUT_PRICE_PER_TOKEN = MAX_PUBLISHED_OUTPUT_PRICE_PER_MILLION_TOKENS / Decimal(1_000_000)
