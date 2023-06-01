"""
This conftest.py contains handy components that prepare SparkSession and other relevant objects.
"""

import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import mlflow
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from discoverx.classification import DeltaTable
from discoverx.dx import Classifier
from discoverx.classification import func
from discoverx.dx import Scanner


@dataclass
class FileInfoFixture:
    """
    This class mocks the DBUtils FileInfo object
    """

    path: str
    name: str
    size: int
    modificationTime: int


class DBUtilsFixture:
    """
    This class is used for mocking the behaviour of DBUtils inside tests.
    """

    def __init__(self):
        self.fs = self

    def cp(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        copy_func(src, dest)

    def ls(self, path: str):
        _paths = Path(path).glob("*")
        _objects = [
            FileInfoFixture(str(p.absolute()), p.name, p.stat().st_size, int(p.stat().st_mtime)) for p in _paths
        ]
        return _objects

    def mkdirs(self, path: str):
        Path(path).mkdir(parents=True, exist_ok=True)

    def mv(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        shutil.move(src, dest, copy_function=copy_func)

    def put(self, path: str, content: str, overwrite: bool = False):
        _f = Path(path)

        if _f.exists() and not overwrite:
            raise FileExistsError("File already exists")

        _f.write_text(content, encoding="utf-8")

    def rm(self, path: str, recurse: bool = False):
        deletion_func = shutil.rmtree if recurse else os.remove
        deletion_func(path)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    logging.info("Configuring Spark session for testing environment")
    warehouse_dir = tempfile.TemporaryDirectory().name
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)

    _builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.warehouse.dir", Path(warehouse_dir).as_uri())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
    )
    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logging.info("Spark session configured")
    yield spark
    logging.info("Shutting down Spark session")
    spark.stop()
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)


    

@pytest.fixture(autouse=True, scope="module")
def sample_datasets(spark: SparkSession, request):
    """
    This fixture loads a sample dataset defined in a csv and
    creates a table registered in the metastore to be used for
    tests.

    Args:
        spark: Spark session
        request: the pytest request fixture contains information about
            the current test. Used here to get current path.

    Returns:

    """
    logging.info("Creating sample datasets")

    module_path = Path(request.module.__file__)

    warehouse_dir = tempfile.TemporaryDirectory().name
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)

    # tb_1
    test_file_path = module_path.parent / "data/tb_1.csv"
    (spark
        .read
        .option("header", True)
        .schema("id integer,ip string,mac string,description string")
        .csv(str(test_file_path.resolve()))
    ).createOrReplaceTempView("view_tb_1")
    spark.sql(f"CREATE TABLE IF NOT EXISTS default.tb_1 USING delta LOCATION '{warehouse_dir}/tb_1' AS SELECT * FROM view_tb_1 ")

    # tb_2
    test_file_tb2_path = module_path.parent / "data/tb_2.json"
    schema_json_example = (
    StructType()
    .add(
        "customer",
        StructType()
        .add("name", StringType(), True)
        .add("id", IntegerType(), True)
        .add(
            "contact",
            StructType()
            .add(
                "address",
                StructType()
                .add("street", StringType(), True)
                .add("town", StringType(), True)
                .add("postal_number", StringType(), True)
                .add("country", StringType(), True)
                .add("ips_used", ArrayType(StringType()), True),
                True,
            )
            .add("email", StringType()),
        )
        .add("products_owned", ArrayType(StringType()), True)
        .add("interactions", MapType(StringType(), StringType())),
        True,
    )
    .add("active", BooleanType(), True)
    .add("categories", MapType(StringType(), StringType()))
)
    spark.read.schema(schema_json_example).json(str(test_file_tb2_path.resolve())).createOrReplaceTempView("view_tb_2")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS default.tb_2 USING delta LOCATION '{warehouse_dir}/tb_2' AS SELECT * FROM view_tb_2 ")
    # columns_mock
    test_file_path = module_path.parent / "data/columns_mock.csv"
    (spark
        .read
        .option("header", True)
        .schema("table_catalog string,table_schema string,table_name string,column_name string,data_type string,partition_index int")
        .csv(str(test_file_path.resolve()))
    ).createOrReplaceTempView("view_columns_mock")
    spark.sql(f"CREATE TABLE IF NOT EXISTS default.columns_mock USING delta LOCATION '{warehouse_dir}/columns_mock' AS SELECT * FROM view_columns_mock")
    

    logging.info("Sample datasets created")

    yield None

    logging.info("Test session finished, removing sample datasets")

    spark.sql("DROP TABLE IF EXISTS default.tb_1")
    spark.sql("DROP TABLE IF EXISTS default.tb_2")
    spark.sql("DROP TABLE IF EXISTS default.columns_mock")
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)

@pytest.fixture(scope="session", autouse=True)
def mlflow_local():
    """
    This fixture provides local instance of mlflow with support for tracking and registry functions.
    After the test session:
    * temporary storage for tracking and registry is deleted.
    * Active run will be automatically stopped to avoid verbose errors.
    :return: None
    """
    logging.info("Configuring local MLflow instance")
    tracking_uri = tempfile.TemporaryDirectory().name
    registry_uri = f"sqlite:///{tempfile.TemporaryDirectory().name}"

    mlflow.set_tracking_uri(Path(tracking_uri).as_uri())
    mlflow.set_registry_uri(registry_uri)
    logging.info("MLflow instance configured")
    yield None

    mlflow.end_run()

    if Path(tracking_uri).exists():
        shutil.rmtree(tracking_uri)

    if Path(registry_uri).exists():
        Path(registry_uri).unlink()
    logging.info("Test session finished, unrolling the MLflow instance")


@pytest.fixture(scope="module")
def monkeymodule():
    """
    Required for monkeypatching with module scope.
    For more info see
    https://stackoverflow.com/questions/53963822/python-monkeypatch-setattr-with-pytest-fixture-at-module-scope
    """
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="module")
def mock_vacuum(spark, monkeymodule):
    def vacuum(self):
        do_nothing = None

    monkeymodule.setattr(DeltaTable, "vacuum", vacuum)


@pytest.fixture(autouse=True, scope="module")
def mock_uc_functionality(spark, monkeymodule):
    # apply the monkeypatch for the columns_table_name
    monkeymodule.setattr(Scanner, "COLUMNS_TABLE_NAME", "default.columns_mock")

    # mock classifier method _get_classification_table_from_delta as we don't
    # have catalogs in open source spark
    def get_classification_table_mock(self):
        (schema, table) = self.classification_table_name.split(".")
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
        self.spark.sql(
            f"""
                CREATE TABLE IF NOT EXISTS {schema + '.' + table} (table_catalog string, table_schema string, table_name string, column_name string, data_type string, tag_name string, effective_timestamp timestamp, current boolean, end_timestamp timestamp) USING DELTA
                """
        )
        return DeltaTable.forName(self.spark, self.classification_table_name)

    monkeymodule.setattr(Classifier, "_get_classification_table_from_delta", get_classification_table_mock)

    # mock UC's tag functionality
    def set_uc_tags(self, series):
        if (series.action == "to_be_set"):
            logging.debug(
                f"Set tag {series.tag_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )
        if (series.action == "to_be_unset"):
            logging.debug(
                f"Unset tag {series.tag_name} for column {series.column_name} of table {series.table_catalog}.{series.table_schema}.{series.table_name}"
            )

    monkeymodule.setattr(Classifier, "_set_tag_uc", set_uc_tags)

    yield

    spark.sql("DROP TABLE IF EXISTS _discoverx.tags")
    spark.sql("DROP SCHEMA IF EXISTS _discoverx")


@pytest.fixture(scope="module")
def mock_current_time(spark, monkeymodule):
    def set_time():
        return func.to_timestamp(func.lit("2023-01-01 00:00:00"))

    monkeymodule.setattr(func, "current_timestamp", set_time)
