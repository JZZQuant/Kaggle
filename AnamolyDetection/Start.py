from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
import sys


def yarn_job():
    train = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('data/train.csv')
    train.registerTempTable("train")
    sqlContext.sql("select v22,count(target) from train group by v22").show()

if __name__ == "__main__":
    spark_home = os.environ.get("SPARK_HOME")
    sys.path.insert(0, spark_home + "/python")
    os.environ["PYSPARK_SUBMIT_ARGS"]='--master local --executor-memory 2g --packages com.databricks:spark-csv_2.10:1.3.0 pyspark-shell'
    sys.path.insert(0, os.path.join(spark_home, "python/lib/py4j-0.8.2.1-src.zip"))
    execfile(os.path.join(spark_home, "python/pyspark/shell.py"))
    yarn_job()
    print "Successfully launched PySpark, Press Enter to Finish the Session:"
    raw_input()

