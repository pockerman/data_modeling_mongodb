import findspark
findspark.init(spark_home="/home/alex/MySoftware/spark-3.0.1-bin-hadoop2.7")

from pathlib import Path

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark_sess = None


def load_data(filename: Path, header=True, to_drop=[]):

    global spark_sess
    df_load = spark_sess.read.csv(str(filename), header=header)

    df_load.show(10)

    if len(to_drop) != 0:
        df_load = df_load.drop(*to_drop)

def preprocess_data(df):

    # convert the 'Date' field of df into a
    # time-stamp of the form 'dd/MM/yyyy'
    df = df.withColumn("Year", year(to_timestamp('Date', 'dd/MM/yyyy')))

if __name__ == '__main__':

    n_workers = 3
    app_name = "QUAKE_ETL"
    mongo_spark_connector = 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.1'


    # configure spark session
    spark_sess = SparkSession\
        .builder\
        .master('local[' + str(n_workers) + ']')\
        .appName(app_name)\
        .config('spark.jars.packages', mongo_spark_connector)\
        .getOrCreate()
    load_data(filename=Path('../data/database.csv'))