#!/usr/bin/env python3
import aws_cdk as cdk

from cdk_app.cdk_app_stack import CdkAppStack

app = cdk.App()

#deploy RearcDataQuestStack inside cdk within the env provided
CdkAppStack(
    app,
    "RearcDataQuestStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region"),
    ),
)
# convert into a aws friendly deployment plan
app.synth()
