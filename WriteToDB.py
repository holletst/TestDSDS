def write_to_db(sdataframe_data, jdbcUrl, db_table, Schema_from_validation):
    df_schema_data = StructType(
        [StructField("entity", StringType(), True),
         StructField("key_value", StringType(), True),
         StructField("metadata_version", StringType(), False),
         StructField("source_action", StringType(), False),
         StructField("submitted", DateType(), False),
         StructField("processed", TimestampType(), True),
         StructField("json_data", StringType(), True),
         # evtl. vorher auf das kommende df json_data(typ struct) to_json anwenden, dann schema anwenden
         StructField("raw_filename", StringType(), True),
         StructField("flag_anonym", IntegerType(), False),
         StructField("anonymized", DateType(), False)])

    # Schema aus Schema-Abgleich übertragen
    sdf_dw_data = spark.createDataFrame(sdataframe_data.rdd, schema=df_schema_data)

    sdf_dw_data.write \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", db_table) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

    # metadata = df.select('<@metadata>.*') # Alle Metdaten in ein neues Dataframe einfügen und in Metadata-Tabelle schreiben

    return sdf_dw_data


sdf_write = write_to_db(transform_df(sdf, filename), jdbcUrl, "integration.crm2", "hier das Schema")
display(sdf_write)
