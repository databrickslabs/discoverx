import logging
import re


class Logging:
    def friendly(self, message):
        print(re.sub("<[^<]+?>", "", message))
        logging.info(message)

    def friendlyHTML(self, message):
        try:
            from dbruntime.display import displayHTML  # pylint: disable=import-error

            displayHTML(message)
        except:
            # Strip HTML classes
            print(re.sub("<[^<]+?>", "", message))
        logging.info(message)

    def info(self, message):
        print(message)
        logging.info(message)

    def debug(self, message):
        logging.debug(message)

    def error(self, message):
        print(message)
        logging.error(message)
