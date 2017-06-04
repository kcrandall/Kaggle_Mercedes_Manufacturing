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
from feature_combiner import feature_combiner

sc = SparkContext(appName="App")
sqlContext = SQLContext(sc)


Y            = 'y'
ID_VAR       = 'ID'
DROPS        = [ID_VAR]

train = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://emr-related-files/train.csv')
test = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://emr-related-files/test.csv')
train.show()
print(train.count)
#Work around for splitting wide data, you need to split on an ID varaible
(train1,valid1) = train.select(ID_VAR).randomSplit([0.7,0.3], seed=123)
train = train1.join(train,ID_VAR,'inner')
valid = valid1.join(train, ID_VAR,'inner')

train.show()

original_nums, cats = get_type_lists(frame=train,rejects=[ID_VAR,Y],frame_type='spark') #These three have test varaibles that don't occur in the train dataset



print("Encoding numberic variables...")
training_df_list, test_df_list,valid_df_list = list(),list(),list()
for i, var in enumerate(cats):
    total = len(cats)

    print('Encoding: ' + var + ' (' + str(i+1) + '/' + str(total) + ') ...')

    tr_enc, _ = target_encoder(train, test, var, Y,frame_type='spark',id_col=ID_VAR)
    v_enc, ts_enc = target_encoder(valid, test, var, Y,frame_type='spark',id_col=ID_VAR)
    training_df_list.append(tr_enc)
    test_df_list.append(ts_enc)
    valid_df_list.append(v_enc)
#join all the new variables
for i, df in enumerate(training_df_list):
    train = train.join(training_df_list[i],ID_VAR,'inner')
    test = test.join(test_df_list[i],ID_VAR,'inner')
    valid = valid.join(valid_df_list[i],ID_VAR,'inner')

# print(train.rdd.collect())
print('Done encoding.')


encoded_nums, cats = get_type_lists(frame=train,rejects=[ID_VAR,Y],frame_type='spark')

print('Combining features....')
train, _ = feature_combiner(train, test, encoded_nums, frame_type='spark')
valid, test = feature_combiner(valid, test, encoded_nums, frame_type='spark')
print('Done combining features.')

encoded_combined_nums, cats = get_type_lists(frame=train,rejects=[ID_VAR,Y],frame_type='spark')
