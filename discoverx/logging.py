import logging
class Logging:
    def friendly(self, message):
        print(message)
        logging.info(message)

    def info(self, message):
        print(message)
        logging.info(message)

    def debug(self, message):
        logging.debug(message)

    def error(self, message):
        print(message)
        logging.error(message)
    
