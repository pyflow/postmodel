# coding: utf8
from setuptools import setup

setup(
    name="postmodel",
    install_requires=[
        "pypika>=0.35.21",
        "ciso8601>=2.1.2",
        "basepy>=0.3.1",
        "asyncpg>=0.20.1",
        "contextvars>=2.4;python_version<'3.7'",
    ],
    extras_require={
        'dev':[
            "pytest",
            "pytest-asyncio"
        ]
    },
)
