from pyspark.sql.functions import (col,hour,dayofweek,dayofmonth,month,when,round,abs)

from gridflow_analytics.common.spark_session import get_spark_session
from gridflow_analytics.common.adls_auth import configure_adls
from gridflow_analytics.common.logger import logger
from pyspark.dbutils import DBUtils
from delta.tables import DeltaTable

from gridflow_analytics.config.config import (GOLD_CONTAINER,SILVER_CONTAINER,STORAGE_ACCOUNT,MODELED_SOURCE)


def extract(spark):

    silver_path = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/demand"

    logger.info(f"Reading Electricity Modeled Silver Data: {silver_path}")

    df = spark.read.format("delta").load(silver_path).filter((col("source_type") == "modeled") & (col("source") == MODELED_SOURCE))

    logger.info(f"Modeled Silver Record Count: {df.count()}")

    return df


def transform(df):

    logger.info("Transforming Electricity Modeled data into Gold format.")

    gold_df = (df.withColumn("demand_vs_installed_capacity_pct",round((col("demand_mw") / col("installed_in_state_mw")) * 100,2))
                 .withColumn("hour",hour(col("timestamp")))
                 .withColumn("day_of_week",dayofweek(col("timestamp")))
                 .withColumn("day_of_month",dayofmonth(col("timestamp")))
                 .withColumn("month",month(col("timestamp")))
                 .withColumn("is_weekend",when(dayofweek(col("timestamp")).isin(1,7),True).otherwise(False))
    )

    logger.info(f"Modeled Gold Record Count: {gold_df.count()}")

    return gold_df


def load(spark,df):

    gold_path = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/electricity/modeled_gold"

    logger.info(f"Writing Electricity Modeled Gold Data: {gold_path}")

    gold_df = df.dropDuplicates(["state","timestamp","source"])

    logger.info(f"Modeled Gold Record Count After Deduplication: {gold_df.count()}")

    if DeltaTable.isDeltaTable(spark,gold_path):

        gold_table = DeltaTable.forPath(spark,gold_path)

        (
            gold_table.alias("target")
            .merge(
                gold_df.alias("source"),
                """
                target.state = source.state
                AND target.timestamp = source.timestamp
                AND target.source = source.source
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info("Electricity Modeled Gold Delta MERGE completed successfully.")

    else:

        gold_df.write.format("delta").mode("overwrite").save(gold_path)

        logger.info("Electricity Modeled Gold Delta table initialized successfully.")


def main():

    spark = get_spark_session()
    dbutils = DBUtils(spark)

    try:

        logger.info("Starting Electricity Modeled Silver -> Gold ETL")

        configure_adls(spark,dbutils)

        silver_df = extract(spark)

        gold_df = transform(silver_df)

        load(spark,gold_df)

        logger.info("Electricity Modeled Silver -> Gold ETL completed successfully.")

    except Exception:

        logger.exception("Electricity Modeled Gold ETL failed.")

        raise

    finally:

        logger.info("Electricity Modeled Gold ETL completed.")


if __name__ == "__main__":
    main()