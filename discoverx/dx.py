from discoverx.interaction import Interaction

interaction = Interaction()

def intro():
    interaction.intro()

def help():
    interaction.help()

def scan(catalogs="*", databases="*", tables="*", rules="*", sample_size=10000, output_table=None):
    interaction.scan(catalogs, databases, tables, rules, sample_size, output_table)

def rules():
    interaction.rules()

def results():
    interaction.results()