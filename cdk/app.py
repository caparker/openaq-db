#!/usr/bin/env python3
from pathlib import Path

import aws_cdk
from aws_cdk import (
    Environment,
    Tags,
)

import os
from database_stack import DatabaseStack
from settings import settings

code_dir = (Path(__file__).parent.absolute()).parent.absolute()
env_dir = os.path.join(code_dir, f'.env.{settings.OPENAQ_ENV}')

print(env_dir)
app = aws_cdk.App()

db = DatabaseStack(
    app,
    f"{settings.OPENAQ_ENV}-database",
    codeDirectory=code_dir,
    envPath=env_dir,
    keyName=settings.KEY_NAME,
    sshIpRange=settings.IP_ADDRESS,
)

Tags.of(db).add("Project", settings.OPENAQ_ENV)


app.synth()
