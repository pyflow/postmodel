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

requirements_str = '''pypika>=0.35.21
ciso8601>=2.1.2
basepy>=0.3.1
asyncpg>=0.20.1
contextvars>=2.4;python_version<"3.7"
'''

def requirements() -> list:
    l = requirements_str.splitlines()
    print(l)
    return l


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
    long_description_content_type="text/markdown",
    project_urls={"Documentation": "https://postmodel.readthedocs.io/"},
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 4 - Beta",
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
        "asyncio"
    ),
    # Dependent packages (distributions)
    install_requires=requirements(),
)
