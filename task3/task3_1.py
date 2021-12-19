from pyspark import SparkContext
import os
from pyspark.sql import SparkSession
import pandas as pd

java8_location = 'F:\ghost\jdk1.8'
os.environ['JAVA_HOME'] = java8_location

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("buzhidao").getOrCreate()
    sc=SparkContext.getOrCreate()
    lines=sc.textFile('D:\朱家萱\文档\大学\金融大数据\实验4\spark\\train_data.csv')
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    types=lines.map(lambda line:line.split(',')[9])
    type_counts = types.map(lambda x: (x, 1)).reduceByKey(lambda a,b:a+b)
    type_counts.saveAsTextFile("output3_1")
    output=type_counts.collect()
    sum_type=0
    for (item, count) in output:
        sum_type+=count
    output = [(x,round(y / sum_type,4)) for (x,y) in output]
    test = pd.DataFrame(data=output)
    test.to_csv('3_1.csv',header=False,index=False,encoding='gbk')