"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""
import re
from setuptools import find_packages, setup

# import version
VERSIONFILE = "discoverx/version.py"
version_line = open(VERSIONFILE, "rt").read()
VERSION_PATTERN = r"^__version__ = ['\"]([^'\"]*)['\"]"
parsed_version = re.search(VERSION_PATTERN, version_line, re.M)
if parsed_version:
    version_string = parsed_version.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

PACKAGE_REQUIREMENTS = ["pyyaml"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark",
    "delta-spark",
    "scikit-learn",
    "pandas",
    "mlflow",
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "pylint",
    "black",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.7,<0.8",
]

setup(
    name="discoverx",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools", "wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points={
        "console_scripts": [
            "etl = discoverx.tasks.sample_etl_task:entrypoint",
            "ml = discoverx.tasks.sample_ml_task:entrypoint",
        ]
    },
    version=version_string,
    description="",
    author="",
)
