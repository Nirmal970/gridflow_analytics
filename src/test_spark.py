from src.common.spark_session import get_spark_session

def main():
    spark=get_spark_session()
    print("Spark Version: ",spark.version)
    spark.stop()
    
if __name__=="__main__":
    main()