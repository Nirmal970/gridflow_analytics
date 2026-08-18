from pyspark.sql.functions import max as spark_max
from gridflow_analytics.common.logger import logger
from gridflow_analytics.common.silver_utils import merge_delta


def get_max_ingestion_timestamp(spark, gold_path):
    try:
        gold_df = spark.read.format("delta").load(gold_path)
        max_ts = gold_df.select(spark_max("ingestion_timestamp")).collect()[0][0]
        return True, max_ts
    except:
        return False, None


def read_silver_incremental(spark, silver_path, gold_path, filters=None, transform=None):
    gold_exists, max_ts = get_max_ingestion_timestamp(spark, gold_path)
    df = spark.read.format("delta").load(silver_path)
    if filters:
        for f in filters:
            df = df.filter(f)
    if gold_exists and max_ts is not None:
        logger.info(f"Reading data with ingestion_timestamp > {max_ts}")
        df = df.filter(col("ingestion_timestamp") > max_ts)
    else:
        logger.info("Reading all data (first run)")
    if transform:
        df = transform(df)
    return df


def save_gold(df, gold_path, merge_condition):
    gold_exists, _ = get_max_ingestion_timestamp(df.sparkSession, gold_path)
    if gold_exists:
        merge_delta(df.sparkSession, df, gold_path, merge_condition)
    else:
        df.write.format("delta").mode("overwrite").save(gold_path)