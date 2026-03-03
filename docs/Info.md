### Project Overview
Basically a pipeline in which we collect public  data over 2 workers dump it to our data lake s3, anaytics is taken by databricks/dbt and automation is done using aws cdk

### Architecture (Part 1-4)
1. s3 is our data lake

2. Ingest Lambda([text](src/rearc_data_quest/lambda_handlers/ingest_handler.py)) takes care of our ingestion part 1 and part 2

3. its started daily via EventBridge

4. Analytics Lambda ([text](src/rearc_data_quest/lambda_handlers/analytics_handler.py)) is responsible for part 3 

5. Databricks Notebook provided in the notebooks folder [text](notebooks) reads data from s3 creates staging 
tables runs dbt models and test

6. Secret Manager is storing all the sensitive creds

7. CloudWatch Logs is added and integrate 1 run_id that helps track one full run end-to-end.

8. metadata driven pipelines making production scaling easier

9. config driven design is ensured

### Tech Stack and Why
1. s3 our data lake

2. lambda for part 1 and 2

3. eventbride for scheduling the ingest lambda

4. aws cdk for reproducible infra and easy redep and destroy

5. datbaricks for analytics compute

6. for creating business views for prod and dev envs and also getting lineage and testability on the data in the analytics layer

### Repo Structure
1. src/rearc_data_quest/
Main application code

2. src/rearc_data_quest/jobs/
Data job logic by part:

part1_bls_sync.py -> sync BLS files to S3
part2_population_api.py -> fetch API JSON to S3

3. src/rearc_data_quest/lambda_handlers/
AWS Lambda entry points:

ingest_handler.py -> runs Part 1 + Part 2
analytics_handler.py -> triggers Databricks job API

4. src/rearc_data_quest/config.py
Central environment-driven settings (Settings.from_env())

5. src/rearc_data_quest/http_utils.py
Lightweight HTTP session/get helpers

6. src/rearc_data_quest/logging_utils.py
Logging setup, run_id correlation, local rotating log support.

7. infra/cdk_app/
Infrastructure as Code using AWS CDK:

app.py -> CDK app entry
cdk_app/cdk_app_stack.py -> defines Lambdas, schedule, secret permissions

8. rearc_dbt/
dbt project for Part 3 transformations

9. tests/
Unit tests for core logic (parsing, config behavior, key generation, etc).

10. .env.example
local environment variables (non-secret defaults).


### Configuration (.env + Secrets)
The requiste pat token are stored in databricks

### How to Run Part 1, 2, 3
Manual part-by-part execution is optional.
Primary execution is automated pipeline run

### How To Run part 4
Running part 4 runs 1,2,3 and we do as follows

1. cd infra/cdk_app

2. source .venv/bin/activate

3. AWS_PROFILE=newacct cdk bootstrap aws://$(AWS_PROFILE=newacct aws sts get-caller-identity --query Account --output text)/ap-south-1

4. AWS_PROFILE=newacct cdk deploy RearcDataQuestStack --require-approval never -c account=$(aws sts get-caller-identity --profile newacct --query Account --output text) -c region=ap-south-1 -c bucketName=<bucket-name>

5. IFN=$(AWS_PROFILE=newacct aws lambda list-functions --region ap-south-1 --query "Functions[?contains(FunctionName, 'RearcDataQuestStack-IngestLambda')].FunctionName | [0]" --output text)

6. AWS_PROFILE=newacct aws lambda invoke --region ap-south-1 --function-name "$IFN" --invocation-type Event /tmp/ingest_async_out.json

7. AWS_PROFILE=newacct aws logs tail "/aws/lambda/$IFN" --since 30m --region ap-south-1

8. AFN=$(AWS_PROFILE=newacct aws lambda list-functions --region ap-south-1 --query "Functions[?contains(FunctionName, 'RearcDataQuestStack-AnalyticsLambda')].FunctionName | [0]" --output text)


9. AWS_PROFILE=newacct aws logs tail "/aws/lambda/$AFN" --since 30m --region ap-south-1







### End-to-End Run

1. Deploy infrastructure (first time only):
cdk bootstrap 
cdk deploy RearcDataQuestStack

2. Ensure secret exists in AWS Secrets Manager:
name: rearc/databricks/job-trigger
value contains: host, token, job_id

3. Trigger full pipeline:
automatic: wait for daily EventBridge schedule

4. Runtime flow:
IngestLambda runs Part 1 + Part 2, writes raw data to S3
on success, Lambda async destination invokes AnalyticsLambda
AnalyticsLambda calls Databricks Jobs API run-now
Databricks job runs staging + dbt run/test

5. Validate success:
S3 updated:
raw/bls/pr/*
raw/datausa/population_latest.json + snapshot
CloudWatch logs:
ingest log group has same pipeline_run_id
analytics log group has pipeline_run_id + Databricks run_id
Databricks job run status = Succeeded
dbt models/tables refreshed in target schema

6. Failure Testing

Failure order:
Ingest Lambda logs -> Analytics Lambda logs -> Databricks job run logs -> dbt logs/tests.


### Observability and Logging
Using the job run id and the logs that are getting deposited also each pipeline run is being monitored by a run id

### Security Decisions

1. Secrets separation
Databricks credentials  are stored in AWS Secrets Manager (rearc/databricks/job-trigger).
Sensitive values are not hardcoded in code.

2. No sensitive logging
Tokens/secrets are not logged.
Error logging in analytics handler sanitizes token-like patterns before writing logs.

3. Configuration hygiene
Non-sensitive settings (bucket name, prefixes, URLs, region) are env-based via Settings.
.env is local-only and not committed.

4. Access control boundaries
Databricks job is triggered through API using PAT scope required for Jobs API.
Token rotation is supported by updating secret value without code change.

### Known Trade-offs / Limitations

1. Quest Part 4 asks for S3 -> SQS -> analytics Lambda. 

Current version uses async Lambda chaining for simplicity, production alignment would replace this with S3 notification -> SQS -> analytics Lambda with DLQ.

2. No DLQ/retry strategy for analytics trigger
Like Failures are visible in logs, but no dedicated DLQ path yet.

3. Databricks Free Edition constraints
Like in the notebook we have used the databrikcs pat directly (which we have rotated but in production we will use databricks secrets)

4. Tests could have have more heavier



### Production Hardening Plan (Prioritized)

1. Queue-based decoupling:
   Implement S3 event notifications to SQS and consume via analytics Lambda with DLQ.
2. Observability:
   Add CloudWatch alarms/metrics for Lambda errors, queue age, and downstream trigger failures.
3. Security:
   Remove hardcoded identifiers, tighten IAM to least privilege, and enforce secret rotation.
4. Data quality:
   Expand dbt tests (freshness, uniqueness, accepted values) and add payload contract checks.


### Additional Images Provide in doc named Supporting showcasing various stages of the run


