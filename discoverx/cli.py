import json
import logging
import sys

from databricks.connect.session import DatabricksSession
from discoverx import DX


logger = logging.getLogger('databricks.labs.discoverx')

def scan(spark, from_tables: str = '*.*.*', rules: str = '*', sample_size: str = '10000', what_if: str = 'false', locale='US'):
    logger.info(f'scan: from_tables={from_tables} rules={rules}')
    dx = DX(spark=spark, locale=locale)
    dx.scan(from_tables=from_tables, rules=rules, sample_size=int(sample_size), what_if='true' == what_if)
    print(dx.scan_result.head())


MAPPING = {
    'scan': scan,
}


def main(raw):
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel('DEBUG')
    logging.root.addHandler(console_handler)

    payload = json.loads(raw)
    command = payload['command']
    if command not in MAPPING:
        raise KeyError(f'cannot find command: {command}')
    flags = payload['flags']
    log_level = flags.pop('log_level')
    if log_level != 'disabled':
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())

    kwargs = {k.replace('-', '_'): v for k,v in flags.items()}

    try:
        spark = DatabricksSession.builder.getOrCreate()
        MAPPING[command](spark, **kwargs)
    except Exception as e:
        logger.error(f'ERROR: {e}')
        logger.debug(f'Failed execution of {command}', exc_info=e)


if __name__ == "__main__":
    main(*sys.argv[1:])
