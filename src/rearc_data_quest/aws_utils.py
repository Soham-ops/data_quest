from __future__ import annotations

import boto3


def s3_client(region_name: str):
    return boto3.client("s3", region_name=region_name)

