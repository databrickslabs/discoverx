from discoverx import logging, explorer
from discoverx.data_model import DataModel
from discoverx.common.helper import strip_margin

class DX:
    def __init__(self):
        self.logger = logging.Logging()
        self.explorer = explorer.Explorer(self.logger)

    def intro(self):
        text = """
        <h1>Hi there, I'm DiscoverX.</h1>

        <p>
          I'm here to help you discover data that matches a set of rules in your lakehouse.<br />
          You can scan a sample of 10000 rows per table from your entire lakehouse by using
        </p>
        <pre><code>dx.scan()</code></pre>
        <p>
          For more detailed instructions, use
        </p>
        <pre><code>dx.help()</code></pre>
        """
        self.logger.friendlyHTML(text)

    def help(self):
        snippet1 = strip_margin("""
          dx.help()  # This will show you this help message

          dx.intro() # This will show you a short introduction to me

          dx.rules() # This will show you the rules that are available to you

          dx.scan()  # This will scan your lakehouse for data that matches a set of rules
        """)
        
        snippet2 = strip_margin("""
          dx.scan(output_table="default.discoverx_results")     # Saves the results in 'discoverx_results' table

          dx.scan(catalogs="*", databases="prod_*", tables="*") # Only scans in databases that start with `prod_`

          dx.scan(databases='prod_*', rules=['phone_number'])   # Only scans for phone numbers in databases that start with `prod_`

          dx.scan(sample_size=100)                              # Samples only 100 rows per table

          dx.scan(sample_size=None)                             # Scan each table for the entire content
        """)
        
        text = f"""
        <h2>I'm glad you asked for help.</h2> 
        <p>
          Here are some things you can do with me:
        </p>
        <pre><code>{snippet1}</code></pre>

        <p>
          Examples of dx.scan() usage: 
        </p>
            
        <pre><code>{snippet2}</code></pre>
        """
        self.logger.friendlyHTML(text)


    def rules(self):
        rules = self.explorer.get_rule_list()
        rule_text = [f"<li>{rule.name} - {rule.description}</li>" for rule in rules]
        rules_text = "\n              ".join(rule_text)
        
        text = f"""
        <h2>Matching rules</h2>
        <p>
          Here are the {len(rules)} rules that are available to you:
        </p>
        <ul>
          {rules_text}
        </ul>
        <p>
          You can also specify your own rules. 
          For example, 
          # TODO
        </p>
        """
        self.logger.friendlyHTML(text)
        
    def scan(self, catalogs="*", databases="*", tables="*", rules="*", sample_size=10000):

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
        
        This may take a while, so please be patient. I'll let you know when I'm done.
        ...
        """
        self.logger.friendly(strip_margin(text))

        self.explorer.scan(table_list, rule_list, sample_size)

        self.logger.friendlyHTML(f"""
        <h2>I've finished scanning your data lake.</h2>
        <p>
          Here is a summary of the results:
            # TODO
        </p>
        
        """)

    def results(self):
        self.logger.friendly("Here are the results:")
        self.explorer.scan_summary()
        self.explorer.scan_details()

