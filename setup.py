"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""
import re
from os import path
from setuptools import find_packages, setup


DESCRIPTION = "DiscoverX - Map and Search your Lakehouse"

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

# import version
with open(path.join(this_directory, "discoverx/version.py"), encoding="utf-8") as f:
    version_line = f.read()
VERSION_PATTERN = r"^__version__ = ['\"]([^'\"]*)['\"]"
parsed_version = re.search(VERSION_PATTERN, version_line, re.M)
if parsed_version:
    version_string = parsed_version.group(1)
else:
    raise RuntimeError("Unable to find version string in discoverx/version.py")

PACKAGE_REQUIREMENTS = ["pyyaml"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark>=3.3.0",
    "delta-spark>=2.2.0",
    "pandas<2.0.0",  # From 2.0.0 onwards, pandas does not support iteritems() anymore, spark.createDataFrame will fail
    "numpy<1.24",  # From 1.24 onwards, module 'numpy' has no attribute 'bool'.
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "pylint",
    "black",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.7,<0.9",
]

setup(
    name="dbl_discoverx",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools", "wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    version=version_string,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://databricks.com/learn/labs",
    author="Erni Durdevic, David Tempelmann",
    author_email="labs@databricks.com",
    license_files=("LICENSE",),
)
