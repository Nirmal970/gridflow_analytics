from pyspark.sql.functions import (col,hour,dayofweek,dayofmonth,month,when,round,abs)

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.logger import logger
from pyspark.dbutils import DBUtils
from delta.tables import DeltaTable

from gridflow_analytics.config.config import (GOLD_CONTAINER,SILVER_CONTAINER,STORAGE_ACCOUNT)

def extract(spark):

    try:

        silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"electricity/demand"

        logger.info(f"Reading Electricity Silver Data: {silver_path}")

        df = spark.read.format("delta").load(silver_path)

        logger.info(f"Silver Record Count: {df.count()}")

        return df

    except Exception:

        logger.exception("Failed during Electricity Silver extraction.")

        raise


def transform(df):

    try:

        logger.info("Transforming Electricity Silver data into Gold format...")
        
        gold_df = df.filter(col("source_type") == "official")

        gold_df = gold_df.withColumn("demand_vs_peak_pct", round((col("demand_mw") / col("peak_mw")) * 100, 2))
        gold_df = gold_df.withColumn("demand_vs_installed_capacity_pct", round((col("demand_mw") / col("installed_in_state_mw")) * 100, 2))
        gold_df = gold_df.withColumn("frequency_deviation_hz", round(abs(col("frequency_hz") - 50.0), 3))
        gold_df = gold_df.withColumn("hour", hour(col("timestamp")))
        gold_df = gold_df.withColumn("day_of_week", dayofweek(col("timestamp")))
        gold_df = gold_df.withColumn("day_of_month", dayofmonth(col("timestamp")))
        gold_df = gold_df.withColumn("month", month(col("timestamp")))
        gold_df = gold_df.withColumn("is_weekend", when(dayofweek(col("timestamp")).isin(1, 7), True).otherwise(False))

        logger.info(f"Gold Record Count: {gold_df.count()}")

        return gold_df

    except Exception:

        logger.exception("Failed during Electricity Gold transformation.")

        raise


def load(spark,df):

    try:

        gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"f"electricity/gold"

        logger.info(f"Writing Electricity Gold Data: {gold_path}")

        gold_df = df.dropDuplicates(["state","timestamp","source"])

        logger.info(f"Gold Record Count After Deduplication: {gold_df.count()}")

        if DeltaTable.isDeltaTable(spark,gold_path):

            gold_table = DeltaTable.forPath(spark,gold_path)

            gold_table.alias("target").merge(
                gold_df.alias("source"),"target.state = source.state AND target.timestamp = source.timestamp AND target.source = source.source"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            logger.info("Electricity Gold Delta MERGE completed successfully.")

        else:

            gold_df.write.format("delta").mode("overwrite").save(gold_path)

            logger.info("Electricity Gold Delta table initialized successfully.")

    except Exception:

        logger.exception("Failed while writing Electricity Gold layer.")

        raise



def main():

    spark = get_spark_session()
    dbutils = DBUtils(spark)

    try:

        logger.info("Starting Electricity Silver -> Gold ETL")

        configure_adls(spark,dbutils)

        silver_df = extract(spark)

        gold_df = transform(silver_df)

        load(spark,gold_df)
        logger.info("Electricity Silver -> Gold ETL completed successfully.")

    except Exception:

        logger.exception("Electricity Gold ETL failed.")

        raise

    finally:

        logger.info("Electricity Gold ETL completed.")


if __name__ == "__main__":
    main()