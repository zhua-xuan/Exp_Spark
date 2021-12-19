from pyspark import SparkContext, SQLContext
import os
from pyspark.sql import SparkSession
import pandas as pd

java8_location = 'F:\ghost\jdk1.8'
os.environ['JAVA_HOME'] = java8_location

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("buzhidao").getOrCreate()
    sc=SparkContext.getOrCreate()
    spark = SQLContext(sc)
    spark_df = spark.read.format("csv").option('header', 'true').option('inferScheme', 'true').load("train_data.csv")
    lines=spark_df.select("user_id","censor_status","work_year")
    lines=lines.fillna("0 year")
    res = lines.rdd.map(
        lambda x: (x.user_id,x.censor_status,x.work_year.split(" ")[-2])).filter(
        lambda y: len(y[2])>1 or (len(y[2])==1 and int(y[2])>5))
    output=res.collect()
    res.saveAsTextFile("output3_3")
    test = pd.DataFrame(data=output)
    test.to_csv('3_3.csv', header=["user_id", "censor_status","work_year"], index=False, encoding='gbk')