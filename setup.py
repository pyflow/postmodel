# coding: utf8
import os
import re
import sys
import warnings

from setuptools import find_packages, setup

if sys.version_info < (3, 6):
    raise RuntimeError("Postmodel requires Python 3.6")


here = os.path.dirname(os.path.abspath(__file__))


def version() -> str:
    init_file = os.path.join(here, "postmodel/__init__.py")
    verstrline = open(init_file, "rt").read()
    mob = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", verstrline, re.M)
    if not mob:
        raise RuntimeError("Unable to find version string")
    return mob.group(1)


def requirements() -> list:
    requirements_file = os.path.join(here, "requirements.txt")
    return open(requirements_file, "rt").read().splitlines()


def long_description() -> str:
    long_description_file = os.path.join(here, "README.md")
    return open(long_description_file, "r").read()


setup(
    # Application name:
    name="postmodel",
    # Version number:
    version=version(),
    # Application author details:
    author="Zhuo Wei",
    author_email="zeaphoo@qq.com",
    # License
    license="MIT License",
    # Packages
    packages=find_packages(include=["postmodel*"]),
    zip_safe=True,
    # Include additional files into the package
    include_package_data=True,
    package_data={},
    # Details
    url="https://github.com/postmodel/postmodel",
    description="Easy async ORM for python, built with relations in mind",
    long_description=long_description(),
    long_description_content_type="text/x-rst",
    project_urls={"Documentation": "https://postmodel.readthedocs.io/"},
    classifiers=[
        "License :: OSI Approved :: MIT Software License",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: PL/SQL",
        "Framework :: AsyncIO",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
    ],
    keywords=(
        "sql postgres psql asyncpg "
        "relational database rdbms "
        "orm object mapper "
        "async asyncio aio"
    ),
    # Dependent packages (distributions)
    install_requires=requirements(),
)
