from datetime import datetime,timezone

from delta.tables import DeltaTable
from pyspark.sql.functions import col,explode_outer,from_json,lit,schema_of_json
from pyspark.sql.types import ArrayType,StructType

from gridflow_analytics.common.logger import logger


def read_bronze_observations(spark,bronze_path,silver_path=None):

    bronze_df = spark.read.json(bronze_path)
    if silver_path is not None and DeltaTable.isDeltaTable(spark,silver_path):
    silver_df = spark.read.format("delta").load(silver_path)
    if "ingestion_timestamp" in silver_df.columns:
        max_ingestion = silver_df.selectExpr("max(ingestion_timestamp) as max_ingestion").first()["max_ingestion"]
        if max_ingestion is not None:
            bronze_df = bronze_df.filter(col("ingestion_timestamp") > lit(max_ingestion))

    if "raw_response" not in bronze_df.columns:

        raise ValueError(f"Bronze dataset does not contain raw_response: {bronze_path}")

    sample = bronze_df.select("raw_response").where(col("raw_response").isNotNull()).first()

    if sample is None:

        logger.info(f"No Bronze records found: {bronze_path}")

        return spark.createDataFrame([],bronze_df.schema)

    sample_json = sample["raw_response"]

    schema = spark.range(1).select(schema_of_json(lit(sample_json)).alias("schema")).first()["schema"]

    parsed_df = bronze_df.withColumn("_parsed",from_json(col("raw_response"),schema))

    parsed_type = parsed_df.schema["_parsed"].dataType

    metadata_columns = []

    if "source" in bronze_df.columns:

        parsed_df = parsed_df.withColumnRenamed("source","bronze_source")
        metadata_columns.append("bronze_source")

    for column in ["dataset","from_timestamp","to_timestamp","hours","ingestion_timestamp"]:

        if column in parsed_df.columns:

            metadata_columns.append(column)

    if isinstance(parsed_type,ArrayType):

        result_df = parsed_df.select(*metadata_columns,explode_outer(col("_parsed")).alias("_record")).select(*metadata_columns,col("_record.*"))

    elif isinstance(parsed_type,StructType):

        field_names = [field.name for field in parsed_type.fields]

        if "items" in field_names and isinstance(parsed_type["items"].dataType,ArrayType):

            result_df = parsed_df.select(*metadata_columns,explode_outer(col("_parsed.items")).alias("_record")).select(*metadata_columns,col("_record.*"))

        elif "data" in field_names and isinstance(parsed_type["data"].dataType,ArrayType):

            result_df = parsed_df.select(*metadata_columns,explode_outer(col("_parsed.data")).alias("_record")).select(*metadata_columns,col("_record.*"))

        else:

            result_df = parsed_df.select(*metadata_columns,col("_parsed.*"))

    else:

        raise ValueError(f"Unsupported Bronze JSON structure: {bronze_path}")

    return result_df


def add_processed_timestamp(df):

    return df.withColumn("processed_timestamp",lit(datetime.now(timezone.utc)))


def merge_delta(spark,df,silver_path,merge_condition):

    if df.limit(1).count() == 0:

        logger.info(f"No records to write: {silver_path}")

        return

    if DeltaTable.isDeltaTable(spark,silver_path):

        silver_table = DeltaTable.forPath(spark,silver_path)

        silver_table.alias("target").merge(df.alias("source"),merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:

        df.write.format("delta").mode("overwrite").save(silver_path)