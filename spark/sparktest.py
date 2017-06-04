# imports
import pandas as pd
import numpy as np
import time
import os

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F #https://stackoverflow.com/questions/39504950/python-pyspark-get-sum-of-a-pyspark-dataframe-column-values

from get_type_lists import get_type_lists
from target_encoder import target_encoder

# spark = SparkSession \
#     .builder \
#     .appName("App") \
#     #.config("spark.some.config.option", "some-value") \
#     .getOrCreate()

sc = SparkContext(appName="PythonWordCount")
sqlContext = SQLContext(sc)


Y            = 'y'
ID_VAR       = 'ID'
DROPS        = [ID_VAR]

train = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://emr-related-files/train.csv')
test = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://emr-related-files/test.csv')

print(train.schema)
print(train.dtypes)

original_numerics, categoricals = get_type_lists(frame=train,rejects=[ID_VAR,Y],frame_type='spark') #These three have test varaibles that don't occur in the train dataset



print("Encoding numberic variables...")
training_df_list, test_df_list = list(),list()
for i, var in enumerate(categoricals):
    total = len(categoricals)

    print('Encoding: ' + var + ' (' + str(i+1) + '/' + str(total) + ') ...')

    tr_enc, ts_enc = target_encoder(train, test, var, Y,id_col=ID_VAR)
    training_df_list.append(tr_enc)
    test_df_list.append(ts_enc)
#join all the new variables 
for i, df in enumerate(training_df_list):
    train = train.join(training_df_list[i],ID_VAR,'inner')
    test = test.join(test_df_list[i],ID_VAR,'inner')
print(train.rdd.collect())
print('Done encoding.')
