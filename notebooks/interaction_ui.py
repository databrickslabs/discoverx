# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook to explore use of ipywidgets for interacting with DiscoverX
# MAGIC 
# MAGIC - Current problem is that UC-enabled clusters and their notebooks don't support running spark in ipywidgets' callback functions (Missing Credential Scope error)
# MAGIC - Thought to explore calling REST API instead but that does not seem to be a proper workaround. Need to fetch a proper token from somewhere and it might lead to problems in workspaces with IP Access Lists enabled as these would need to add the driver to the access list.
# MAGIC - This notebook runs in the `e2-demo-field-eng workspace`
# MAGIC - Some more info in https://databricks.slack.com/archives/C027U33QZ9R/p1673970711362629
# MAGIC - Will need to get more info when this will be possible or if there is any workaround

# COMMAND ----------

import ipywidgets as widgets

# COMMAND ----------

catalog_list = spark.sql("SHOW CATALOGS").collect()

# COMMAND ----------

[catalog.catalog for catalog in catalog_list]

# COMMAND ----------

catalog_widget = widgets.Combobox(
    # value='John',
    placeholder='Choose Catalog',
    options=[catalog.catalog for catalog in catalog_list],
    description='Catalog',
    ensure_option=True,
    disabled=False
)

catalog_widget

# COMMAND ----------

# MAGIC %sql 
# MAGIC USE CATALOG discoverx; SHOW SCHEMAS

# COMMAND ----------

spark.sql("""USE CATALOG discoverx""")
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

catalog_widget = widgets.Dropdown(
    # value='John',
    placeholder='Choose Catalog',
    options=[catalog.catalog for catalog in catalog_list],
    description='Catalog',
)

schema_widget = widgets.Dropdown(
    # value='John',
    placeholder='Choose Schema',
    options=["-"],
    description='Schema',
)

output2 = widgets.Output()

def on_value_change(change):
    with output2:
        print('hej')

def catalog_chosen(change):
  spark.sql(f"USE CATALOG discoverx")
  #schemas = [schema.databaseName for schema in spark.sql("SHOW SCHEMAS").collect()]
  #print("hej")
  
  schema_widget.options = ['1', '2', '3']
  
catalog_widget.observe(catalog_chosen, names='value')
catalog_widget.observe(on_value_change, names='value')


HBox([catalog_widget, schema_widget, output2])

# COMMAND ----------

on_value_change('j')

# COMMAND ----------

dropdown

# COMMAND ----------

from ipywidgets import *
x = Dropdown(options=['z', 'a', 'b'])
y = Dropdown(options=[' - '])
out = Output()

def change_x(*args):
  if x.value=='a':
    y.options=[1, 2, 3]
  else:
    try:
      spark.sql("USE CATALOG discoverx")
    except Exception as e:
      with out:
        print(e)
      y.options=[4, 5, 6]
x.observe(change_x, 'value')

HBox([x,y, out])

# COMMAND ----------


