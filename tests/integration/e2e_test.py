from discoverx.tasks.sample_etl_task import SampleETLTask


def test_etl():
    common_config = {"schema": "default", "table": "sklearn_housing"}
    test_etl_config = {"output": common_config}
    etl_job = SampleETLTask(init_conf=test_etl_config)
    etl_job.launch()
    table_name = f"{test_etl_config['output']['schema']}.{test_etl_config['output']['table']}"
    _count = etl_job.spark.table(table_name).count()
    assert _count > 0
