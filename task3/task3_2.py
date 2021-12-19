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
    lines = spark_df.select("user_id", "year_of_loan", "monthly_payment", "total_loan")
    total_money = lines.rdd.map(
        lambda x: (x.user_id, round((float(x.year_of_loan) * float(x.monthly_payment) * 12 - float(x.total_loan)),4)))
    output=total_money.collect()
    total_money.saveAsTextFile("output3_2")
    test = pd.DataFrame(data=output)
    test.to_csv('3_2.csv', header=False, index=False, encoding='gbk')