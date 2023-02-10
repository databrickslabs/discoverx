from discoverx import logging, explorer
from discoverx.data_model import DataModel
class Interaction:
    def __init__(self):
        self.logger = logging.Logging()
        self.explorer = explorer.Explorer(self.logger)

    def intro(self):
        text = """
        Hi there, I'm DiscoverX.

        I'm here to help you discover data that matches a set of rules in your lakehouse.
        You can scan a sample of 10000 rows per table from your entire lakehouse by using
            
            dx.scan()

        For more detailed instructions, use

            dx.help()
        
        """
        self.logger.friendly(text)

    def help(self):
        text = """
        I'm glad you asked for help. Here are some things you can do with me:

            dx.help() # This will show you this help message
            dx.intro() # This will show you a short introduction to me
            dx.rules() # This will show you the rules that are available to you
            dx.scan() # This will scan your lakehouse for data that matches a set of rules
            
        ----- 

        Examples of dx.scan() usage: 

            You can save the results to a table by using

                dx.scan(output_table="default.discoverx_results")

            Or you can scan a subset of your lakehouse by filtering on catalogs, databases and tables

                dx.scan(catalogs="*", databases="prod_*", tables="*")

            You can also filter on rules. For example, if you want to scan for phone numbers, you can use

                dx.scan(databases='prod_*', rules=['phone_number'])

            You can also scan a different sample size. For example

                dx.scan(sample_size=100)
                dx.scan(sample_size=None) # This will scan the entire table

        """
        self.logger.friendly(text)


    def rules(self):
        rules = self.explorer.get_rule_list()

        text = f"""
        Here are the {len(rules)} rules that are available to you:
        """
        self.logger.friendly(text)

        
        for rule in rules:
            self.logger.friendly(f"            {rule.name} - {rule.description}")

        text = """
        You can also specify your own rules. 
        For example, 
        # TODO
        """
        self.logger.friendly(text)
        
    def scan(self, catalogs, databases, tables, rules, sample_size, output_table):

        table_list = self.explorer.get_table_list(catalogs, databases, tables)
        rule_list = self.explorer.get_rule_list(rules)

        n_catalogs = len(set(map(lambda x: x.catalog, table_list)))
        n_databases = len(set(map(lambda x: x.database, table_list)))
        n_tables = len(table_list)
        n_rules = len(rule_list)

        text = f"""
        Ok, I'm going to scan your lakehouse for data that matches your rules.

        This is what you asked for:

            catalogs ({n_catalogs}) = {catalogs}
            databases ({n_databases}) = {databases}
            tables ({n_tables}) = {tables}
            rules ({n_rules}) = {rules}
            sample_size = {sample_size}
            output_table = {output_table if output_table else ""}

        This may take a while, so please be patient. I'll let you know when I'm done.
        """
        self.logger.friendly(text)

        self.explorer.scan(table_list, rule_list, sample_size, output_table)

        self.logger.friendly(f"""
        I've finished scanning your data lake.
        
        Here is a summary of the results:
            # TODO

        You can see the full results by exploring the table that I saved the results to ({output_table}).
        """)

    def results(self):
        self.logger.friendly("Here are the results:")
        self.explorer.scan_summary()
        self.explorer.scan_details()

