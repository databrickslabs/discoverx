import logging
import re

logger = logging.getLogger('databricks.labs.discoverx')

class Logging:
    def friendly(self, message):
        print(re.sub("<[^<]+?>", "", message))
        logger.info(message)

    def friendlyHTML(self, message):
        try:
            from dbruntime.display import displayHTML  # pylint: disable=import-error

            displayHTML(message)
        except:
            # Strip HTML classes
            print(re.sub("<[^<]+?>", "", message))
        logger.info(message)

    def info(self, message):
        print(message)
        logger.info(message)

    def debug(self, message):
        logger.debug(message)

    def error(self, message):
        print(message)
        logger.error(message)
