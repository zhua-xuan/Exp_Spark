from pyspark import SparkContext
import os
from pyspark.sql import SparkSession

java8_location = 'F:\ghost\jdk1.8'
os.environ['JAVA_HOME'] = java8_location

# Path for spark source folder
#os.environ['SPARK_HOME'] = "F:\ghost\spark\spark-3.2.0-bin-hadoop3.2"
# Append pyspark to Python Path
#sys.path.append("F:\ghost\spark\spark-3.2.0-bin-hadoop3.2\python")

if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("buzhidao").getOrCreate()
    sc=SparkContext.getOrCreate()
    lines=sc.textFile('D:\朱家萱\文档\大学\金融大数据\实验4\spark\\train_data.csv')
    header = lines.first()
    lines = lines.filter(lambda row: row != header)
    lines=lines.map(lambda line:line.split(',')[2])
    counts = lines.map(lambda x: (int((float(x)//1000)*1000), 1)).reduceByKey(lambda a,b:a+b)
    output=counts.collect()
    output.sort(key=lambda x:x[0])
    for (item, count) in output:
        print("((%i,%i),%i)"%(item,item+1000,count))
    counts.saveAsTextFile("output2")