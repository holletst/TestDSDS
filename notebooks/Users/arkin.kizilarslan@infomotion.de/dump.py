# Databricks notebook source
splittet_sdf= customized_sdf.select(customized_sdf[0], customized_sdf[1][0], customized_sdf[2], customized_sdf[3][0], customized_sdf[4], customized_sdf[5], customized_sdf[6][0], customized_sdf[7], customized_sdf[8], customized_sdf[9])

#splittet_sdf = spark.createDataFrame([], customized_sdf.schema) # Der Versuch das Schema zu mappen 

#n= customized_sdf.rdd.take(1)[0][1] # Der Versuch einen dynamischen Range anzulegen
for x in range(1, 16):
  splittet_sdf= splittet_sdf.union(customized_sdf.select(customized_sdf[0], customized_sdf[1][x], customized_sdf[2], customized_sdf[3][x], customized_sdf[4], customized_sdf[5], customized_sdf[6][x], customized_sdf[7], customized_sdf[8], customized_sdf[9]))

display(splittet_sdf)


#Frage: einen "Daten-Move" von einem Storage Acc zum anderen. Über Mounten lösen oder über einfachen df.write Befehl mit anderer spark.conf.set von Storage Acc


# COMMAND ----------

### Mit SQL DW-Connector SQL DW beladen


# Setzen der Verbindung auf temporär Blob Storage
spark.conf.set(
  "fs.azure.account.key.dsdsfordatabrickstmpblob.blob.core.windows.net",
  "f7hKePQaxIRxSFxp6EmdFpKOV/Z7v4+SVsd4Wu+2JxdMGWChtxjvqN1mtMnyiGCAvVoEA+C2lUdrYs8AtxMPEg==")

splittet_sdf_dw.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", jdbcUrl) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "integration.crm4001") \
  .option("tempDir", "wasbs://tmp@dsdsfordatabrickstmpblob.blob.core.windows.net/tmp_directory2") \
  .mode("append") \
  .save()

# COMMAND ----------

current_time = date_format(from_utc_timestamp(current_timestamp().cast(TimestampType()), "GMT+2"), 'dd.MM.yyyy HH:mm:ss')

test_df = sqlContext.createDataFrame(
    [(1, "a", 23.0), (3, "B", -23.0)], ("x1", "x2", "x3"))

test_df = test_df.withColumn("x4", current_time )
test_df.show()

# COMMAND ----------

import pandas as pd
print(pd.__version__)
print(spark.sparkContext.appName," ,", spark.sparkContext.pythonVer," ,", spark.sparkContext.applicationId)

import sys
print(sys.version)