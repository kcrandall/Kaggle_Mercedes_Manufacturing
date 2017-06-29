# imports

import h2o
import numpy as np
import pandas as pd
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.estimators.random_forest import H2ORandomForestEstimator
from h2o.grid.grid_search import H2OGridSearch

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F #https://stackoverflow.com/questions/39504950/python-pyspark-get-sum-of-a-pyspark-dataframe-column-values

from logging_lib.LoggingController import LoggingController

import h2o
h2o.show_progress()                                          # turn on progress bars
from h2o.estimators.glm import H2OGeneralizedLinearEstimator # import GLM models
from h2o.estimators.deeplearning import H2ODeepLearningEstimator
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.estimators.random_forest import H2ORandomForestEstimator
from h2o.grid.grid_search import H2OGridSearch               # grid search
from h2o.estimators.xgboost import H2OXGBoostEstimator
from h2o.estimators.stackedensemble import H2OStackedEnsembleEstimator
import xgboost as xgb
import matplotlib

matplotlib.use('Agg')                                       #Need this if running matplot on a server w/o display
from pysparkling import *


#Define your s3 bucket to load and store data
S3_BUCKET = 'rza-ml-1'

#Create a custom logger to log statistics and plots
logger = LoggingController()
logger.s3_bucket = S3_BUCKET

#.config('spark.executor.cores','6') \
spark = SparkSession.builder \
        .appName("App") \
        .getOrCreate()
        # .master("local[*]") \
        # .config('spark.cores.max','16')
        #.master("local") \
        # .config("spark.some.config.option", "some-value") \

spark.sparkContext.setLogLevel('WARN') #Get rid of all the junk in output

Y            = 'y'
ID_VAR       = 'ID'
DROPS        = [ID_VAR]
#From an XGBoost model

# location of "dirty" file
# decision trees handle dirty data elegantly
#path = ## Read File

# NOTE the top 6 are categorical, might want to look into this.
MOST_IMPORTANT_VARS_ORDERD = ['X5','X0','X8','X3','X1','X2','X314','X47','X118',\
'X315','X29','X127','X236','X115','X383','X152','X151','X351','X327','X77','X104',\
'X267','X95','X142']
#Load data from s3
train = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://'+S3_BUCKET+'/train.csv')
test = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('s3n://'+S3_BUCKET+'/test.csv')

#this needs to be done for h2o glm.predict() bug (which needs same number of columns)
test = test.withColumn(Y,test[ID_VAR])


#Work around for splitting wide data, you need to split on only an ID varaibles
#Then join back with a train varaible (bug in spark as of 2.1 with randomSplit())
train_temp , valid_temp = train.select(ID_VAR).randomSplit([0.7,0.3], seed=123)

valid = valid_temp.join(train,ID_VAR,'inner')

train = train_temp.join(train,ID_VAR,'inner')

# split into 40% training, 30% validation, and 30% test
#train, valid, test = frame.split_frame([0.4, 0.3])



conf = H2OConf(spark=spark)
conf.nthreads = -1
hc = H2OContext.getOrCreate(spark,conf)


print('Making h2o frames...')
train_h20_frame = hc.as_h2o_frame(train, "trainTable")
valid_h20_frame = hc.as_h2o_frame(valid, "validTable")
test_h2o_frame = hc.as_h2o_frame(test, "testTable")
print('Done making h2o frames.')

logger.log_string("Train Summary:")
logger.log_string("Rows:{}".format(train_h20_frame.nrow))
logger.log_string("Cols:{}".format(train_h20_frame.ncol))

X = [name for name in train.columns if name not in ['id', '_WARN_', Y]]


# assign target and inputs
#y = 'bad_loan'
#X = [name for name in frame.columns if name not in ['id', '_WARN_', y]]
#print(y)
#print(X)



# random forest

# initialize rf model
rf_model = H2ORandomForestEstimator(
    ntrees=500,                      # Up to 500 decision trees in the forest
    max_depth=30,                    # trees can grow to depth of 30
    stopping_rounds=5,               # stop after validation error does not decrease for 5 iterations/new trees
    score_each_iteration=True,       # score validation error on every iteration/new tree
    model_id='rf_model')             # for easy lookup in flow

# train rf model
rf_model.train(
    x=X,
    y=Y,
    training_frame=train_h20_frame,
    validation_frame=valid_h20_frame)

# print model information


sub = test_h2o_frame[ID_VAR].cbind(rf_model.predict(test_h2o_frame))
print(sub.head())


# create time stamp
import re
import time
time_stamp = re.sub('[: ]', '_', time.asctime())

# save file for submission
sub.columns = [ID_VAR, Y]
sub_fname = 'Submission_'+str(time_stamp) + '.csv'
# h2o.download_csv(sub, 's3n://'+S3_BUCKET+'/kaggle_submissions/Mercedes/' +sub_fname)

spark_sub_frame = hc.as_spark_frame(sub)

spark_sub_frame.select(ID_VAR,Y).coalesce(1).write.option("header","true").csv('s3n://'+S3_BUCKET+'/Kaggle_Submissions/Mercedes/' +sub_fname)
