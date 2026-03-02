from __future__ import annotations

import os
from dataclasses import dataclass


def _getenv(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


#Settings is a class used to store data 
#@dataclass provideds repetitive setup code for classes that mostly store data
@dataclass(frozen=True)
class Settings:
    aws_region: str
    s3_bucket_name: str
    bls_base_url: str
    bls_user_agent: str
    population_api_url: str
    s3_bls_prefix: str
    s3_api_prefix: str

    #class method signal from_env() belongs to the class itself
    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            s3_bucket_name=_getenv("S3_BUCKET_NAME", "rearc-data-quest-dev"),
            bls_base_url=os.getenv(
                "BLS_BASE_URL", "https://download.bls.gov/pub/time.series/pr/"
            ),
            bls_user_agent=_getenv(
                "BLS_USER_AGENT", "Rearc Data Quest Contact (sohamemail@gmail.com)"
            ),
            population_api_url=os.getenv(
                "POPULATION_API_URL",
                (
                    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
                    "?cube=acs_yg_total_population_1"
                    "&drilldowns=Year%2CNation&locale=en&measures=Population"
                ),
            ),
            s3_bls_prefix=os.getenv("S3_BLS_PREFIX", "raw/bls/pr/"),
            s3_api_prefix=os.getenv("S3_API_PREFIX", "raw/datausa/"),
        )

