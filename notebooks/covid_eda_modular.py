# Databricks notebook source
# MAGIC %md
# MAGIC ### View the latest COVID-19 hospitalization data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get and Transform data

# COMMAND ----------

# MAGIC %pip install tqdm==4.67.0

# COMMAND ----------

# MAGIC %pip uninstall -y tqdm==4.67.0

# COMMAND ----------

import tqdm
tqdm.__version__

# COMMAND ----------

# List installed packages with versions
import pkg_resources

installed_packages = [(d.project_name, d.version) for d in pkg_resources.working_set]
installed_packages.sort()

# Display in DataFrame format (optional)
import pandas as pd
df_packages = pd.DataFrame(installed_packages, columns=['Package', 'Version'])
display(df_packages)


# COMMAND ----------

data_path = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv'
print(f'Data path: {data_path}')

# COMMAND ----------

from covid_analysis.transforms import *
import pandas as pd

df = pd.read_csv(data_path)
df = filter_country(df, country='USA')
df = pivot_and_clean(df, fillna=0)  
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')
# Convert from Pandas to a pyspark sql DataFrame.
df = spark.createDataFrame(df)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Delta Lake
# MAGIC The current schema has spaces in the column names, which are incompatible with Delta Lake.  To save our data as a table, we'll replace the spaces with underscores.  We also need to add the date index as its own column or it won't be available to others who might query this table.

# COMMAND ----------

# Write to Delta Lake with schema evolution enabled
df.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable('neo_zhou.dbx_git_demo.covid_stats')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize

# COMMAND ----------

# Using Databricks visualizations and data profiling
display(spark.table('neo_zhou.dbx_git_demo.covid_stats'))

# COMMAND ----------

# Using python
df.toPandas().plot(figsize=(13,6), grid=True).legend(loc='upper left');
