# import findspark
# findspark.init()
#
# # Or the following command
# findspark.init("/path/to/spark_home")

import pyspark as ps
from pyspark import SparkContext, SparkConf

import os
import sys

spark_home = os.environ.get('SPARK_HOME', None)

if not spark_home:
    print('error spark')

sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\\bin")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python\pyspark")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python\pyspark\sql")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python\pyspark\mllib")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python\lib")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python/lib\pyspark.zip")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python/lib\py4j-0.8.2.1-src.zip")
sys.path.append("C:\Platforms\Apache\spark\spark-2.1.1-bin-hadoop2.7\python/lib\pyspark.zip")

os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local pyspark-shell"


conf = SparkConf().setAppName('MyApp').setMaster('local[2]')#.setMaster('spark://5.6.7.8:7077')
sc = SparkContext(conf=conf)
