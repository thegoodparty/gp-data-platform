"""The boto3 session behind both Bedrock clients.

On Astro the pod comes up as the deployment's Astronomer-managed workload
identity, a role in Astronomer's account. Bedrock, its model access, and its
bill live in GoodParty's account, so when GOLD_MATCH_AWS_ROLE_ARN names a role
there the session assumes it (the people-api loader's shape) and every call
authorizes as us. The assumed credentials refresh themselves: a chained STS
session caps at one hour and a wave-day run outlasts that. With the env unset
the ambient chain is used unchanged, which is what supervised local runs do.
"""

import os
from functools import cache

import boto3
import botocore.session
from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials

ROLE_ARN_ENV = "GOLD_MATCH_AWS_ROLE_ARN"
EXTERNAL_ID_ENV = "GOLD_MATCH_AWS_EXTERNAL_ID"


@cache
def bedrock_session(region: str) -> boto3.Session:
    base = boto3.Session(region_name=region)
    role_arn = os.environ.get(ROLE_ARN_ENV)
    if not role_arn:
        return base
    extra_args: dict[str, str] = {"RoleSessionName": "gold-match"}
    if external_id := os.environ.get(EXTERNAL_ID_ENV):
        extra_args["ExternalId"] = external_id
    # The source credentials are the pod's own (web-identity) chain, itself
    # refreshable, so re-assumption keeps working across token rotations.
    botocore_base = base._session
    fetcher = AssumeRoleCredentialFetcher(
        client_creator=botocore_base.create_client,
        source_credentials=botocore_base.get_credentials(),
        role_arn=role_arn,
        extra_args=extra_args,
    )
    assumed = botocore.session.Session()
    assumed._credentials = DeferredRefreshableCredentials(method="assume-role", refresh_using=fetcher.fetch_credentials)
    assumed.set_config_variable("region", region)
    return boto3.Session(botocore_session=assumed)
