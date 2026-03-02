import os

from aws_cdk import (
    Duration,
    Stack,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as _lambda,
    aws_lambda_destinations as lambda_destinations,
    aws_logs as logs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class CdkAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = self.node.try_get_context("bucketName") or os.getenv("S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "Missing bucket name. Pass -c bucketName=<your-bucket> to CDK deploy "
                "or set S3_BUCKET_NAME in environment."
            )

        # Use existing assignment bucket.
        bucket = s3.Bucket.from_bucket_name(
            self,
            "AssignmentBucket",
            bucket_name,
        )

        # Lambda: runs Part 1 + Part 2 (daily).
        ingest_lambda = _lambda.Function(
            self,
            "IngestLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="rearc_data_quest.lambda_handlers.ingest_handler.handler",
            code=_lambda.Code.from_asset("../../src"),
            timeout=Duration.minutes(5),
            memory_size=512,
            log_retention=logs.RetentionDays.TWO_WEEKS,
            environment={
                "S3_BUCKET_NAME": bucket_name,
                "S3_BLS_PREFIX": "raw/bls/pr/",
                "S3_API_PREFIX": "raw/datausa/",
                "LOG_LEVEL": "INFO",
                # Update with your real contactable user-agent before deploy
                "BLS_USER_AGENT": "Rearc Data Quest Contact (you@example.com)",
            },
        )

        # Lambda: runs analytics trigger/logic.
        analytics_lambda = _lambda.Function(
            self,
            "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="rearc_data_quest.lambda_handlers.analytics_handler.handler",
            code=_lambda.Code.from_asset("../../src"),
            timeout=Duration.minutes(5),
            memory_size=512,
            log_retention=logs.RetentionDays.TWO_WEEKS,
            environment={
                "S3_BUCKET_NAME": bucket_name,
                "DATABRICKS_SECRET_NAME": "rearc/databricks/job-trigger",
                "LOG_LEVEL": "INFO",
            },
        )

        databricks_secret = secretsmanager.Secret.from_secret_name_v2(
            self,
            "DatabricksJobTriggerSecret",
            "rearc/databricks/job-trigger",
        )

        # Permissions
        bucket.grant_read_write(ingest_lambda)
        bucket.grant_read(analytics_lambda)
        databricks_secret.grant_read(analytics_lambda)

        # Daily schedule for ingest lambda.
        events.Rule(
            self,
            "DailyIngestSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(ingest_lambda)],
        )

        # Chain analytics after successful async ingest invocation.
        _lambda.EventInvokeConfig(
            self,
            "IngestOnSuccessConfig",
            function=ingest_lambda,
            on_success=lambda_destinations.LambdaDestination(analytics_lambda),
            retry_attempts=0,
        )
