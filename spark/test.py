from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
import os

from pysparkling import *
import h2o
# --conf spark.dynamicAllocation.enabled=false
# http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/8/index.html
# https://pypi.python.org/pypi/h2o_pysparkling_2.1/2.1.7
# wget http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/8/sparkling-water-2.1.8.zip
if __name__ == "__main__":
    # Start SparkContext
    sc = SparkContext(appName="PythonWordCount")
    hc = H2OContext.getOrCreate(sparkSession)

    # Load data from S3 bucket
    lines = sc.textFile('s3n://emr-related-files/words.txt', 1)
    # Calculate word counts
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    # Print word counts
    for (word, count) in output:
        print("%s: %i" % (word, count))
    # Save word counts in S3 bucket
    counts.saveAsTextFile("s3n://emr-related-files/output.txt")
    # Stop SparkContext
    sc.stop()
