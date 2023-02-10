from discoverx import logging
from discoverx import explorer

class Interaction:
    def __init__(self):
        self.logger = logging.Logging()
        self.explorer = explorer.Explorer(self.logger)

    def help(self):
        self.logger.friendly("Hi there, I'm DiscoverX. I'm here to help you discover data in your data lake.")
        self.logger.friendly("I can help you find data that matches a set of rules.")
        self.logger.friendly("For example, you can ask me to find all the columns that contain phone numbers in all databases that start with 'prod_' like so:")
        self.logger.friendly("dx.scan(databases='prod_*', rules=['phone_number'])")

    def scan(self, catalogs="*", databases="*", tables="*", rules="*", sample_size=10000):
        self.logger.friendly("I'm scanning your data lake for data that matches your rules.")
        self.logger.friendly("This may take a while, so please be patient.")
        self.logger.friendly("I'll let you know when I'm done.")

        self.explorer.scan(catalogs, databases, tables, rules, sample_size)

        self.logger.friendly("I've finished scanning your data lake.")

        self.logger.friendly("I have saved the results to a table called 'discoverx.rules_match_frequency'.")

        self.logger.friendly("You can now use the 'dx.results()' command to see the detailed results.")

    def results(self):
        self.logger.friendly("Here are the results:")
        self.explorer.scan_summary()
        self.explorer.scan_details()