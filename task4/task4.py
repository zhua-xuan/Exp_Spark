import pyspark.sql.types as typ
import pyspark.ml.classification as cl
import pyspark.ml.evaluation as ev
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SQLContext
import os
import matplotlib.pyplot as plt

java8_location = 'F:\ghost\jdk1.8'
os.environ['JAVA_HOME'] = java8_location

if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    spark = SQLContext(sc)
    spark_df = spark.read.format("csv").option('header', 'true').option('inferScheme', 'true').load("train_data.csv")
    for col in ['loan_id','user_id']:  #去除无关列
        spark_df = spark_df.drop(col)
    spark_df = spark_df.na.fill("0")   #填充缺失值
    StrLabel=["class","sub_class","work_type","issue_date","employer_type","industry","earlies_credit_mon","work_year"]
    for item in StrLabel:
        indexer=StringIndexer(inputCol=item,outputCol="%sIndex"%item)
        spark_df=indexer.fit(spark_df).transform(spark_df)
        spark_df=spark_df.drop(item)
    for item in spark_df.columns:
        spark_df=spark_df.withColumn(item,spark_df[item].cast(typ.DoubleType()))
    cols=spark_df.columns
    cols.remove("is_default")
    ass = VectorAssembler(inputCols=cols, outputCol="features")
    spark_df=ass.transform(spark_df)
    model_df = spark_df.select(["features","is_default"])
    y_svm=[]
    y_rf=[]
    y_lr=[]
    #按9:1到5：5的比例，分割训练集与测试集,随机数种子设置为959
    for r in (0.5,0.6,0.7,0.8,0.9):
        train_df,test_df=model_df.randomSplit([r,1-r],seed=959)
        print('When the ratio is',10 * r,'to',10 * (1-r) )

        svm = cl.LinearSVC(labelCol='is_default').fit(train_df)
        predicted_svm = svm.transform(test_df)
        svm_accuracy = ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(predicted_svm)
        y_svm.append(svm_accuracy)
        print('Accuracy of SVM on the test set is : {: .2f} %'.format(100 * svm_accuracy))

        rf = cl.RandomForestClassifier(labelCol="is_default", maxBins=700, numTrees=50, maxDepth=7).fit(train_df)
        predicted_rf = rf.transform(test_df)
        rf_accuracy = ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(predicted_rf)
        y_rf.append(rf_accuracy)
        print('Accuracy of RandomForest on the test set is : {: .2f} %'.format(100 * rf_accuracy))

        lr = cl.LogisticRegression(labelCol='is_default').fit(train_df)
        predicted_lr = lr.transform(test_df)
        lr_accuracy=ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(predicted_lr)
        y_lr.append(lr_accuracy)
        print('Accuracy of LogisticRegression on the test set is : {: .2f} %'.format(100 * lr_accuracy))

    x = range(1, len(y_svm) + 1)
    plt.plot(x, y_svm, marker='o', mec='r', mfc='w', label=u'svm_accuracy')
    plt.plot(x, y_rf, marker='*', ms=10, label=u'rf_accuracy')
    plt.plot(x, y_lr, marker='o', ms=10, label=u'lr_accuracy')
    plt.ylim(0.7, 0.9)  # 限定纵轴的范围
    plt.xlim(1, 5)  # 限定横轴的范围
    plt.legend()  # 让图例生效
    plt.margins(0)
    plt.subplots_adjust(bottom=0.15)
    plt.title("auccuracy")  # 标题
    plt.show()