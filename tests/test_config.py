import os

from rearc_data_quest.config import Settings

#monkeypatch is a pytest helper that lets the test temporarily change things (like environment variables) safely.
def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("S3_BUCKET_NAME", "unit-test-bucket")
    monkeypatch.setenv("BLS_USER_AGENT", "Unit Test (unit@test.local)")
    settings = Settings.from_env()

    assert settings.s3_bucket_name == "unit-test-bucket"
    assert settings.aws_region == os.getenv("AWS_REGION", "us-east-1")
    assert settings.s3_bls_prefix.startswith("raw/")

