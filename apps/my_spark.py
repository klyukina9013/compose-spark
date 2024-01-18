from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg


def init_spark():
  sql = SparkSession.builder\
    .appName("spark-house-prices")\
    .config("spark.jars", "/opt/spark-apps/postgresql-42.7.1.jar")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def main():
    url = "jdbc:postgresql://demo-database:5432/spark_house"
    properties = {
    "user": "postgres",
    "password": "dfltpwd",
    "driver": "org.postgresql.Driver"
    }     
    sql,sc = init_spark()
    #TODO load postgres table via jdbc
    df = sql.read.jdbc(url=url, table="real_estate", properties=properties)
    
    df.printSchema()
  
  # Calculate num_rooms as the sum of bath and bedrooms
    df = df.withColumn("num_rooms", col("baths") + col("bedrooms"))

    # Perform the analysis to calculate the average cost
    result = df.groupBy("location", "num_rooms") \
        .agg(avg("price").alias("average_cost"))

    result.show()
    # TODO Save the result to a output table via jdbc
    

    # Stop the Spark session
    sc.stop()
  
if __name__ == '__main__':
  main()