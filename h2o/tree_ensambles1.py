import h2o
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from h2o.estimators.random_forest import H2ORandomForestEstimator
from h2o.grid.grid_search import H2OGridSearch
from h2o.estimators.xgboost import H2OXGBoostEstimator
from h2o.estimators.stackedensemble import H2OStackedEnsembleEstimator
import xgboost as xgb
h2o.init() # give h2o as much memory as possible
h2o.no_progress() # turn off h2o progress bars

import os
import numpy as np
import pandas as pd

from get_type_lists import get_type_lists
from target_encoder import target_encoder
from feature_combiner import feature_combiner


Y            = 'y'
ID_VAR       = 'ID'
DROPS        = [ID_VAR]
#From an XGBoost model
# NOTE the top 6 are categorical, might want to look into this.
MOST_IMPORTANT_VARS_ORDERD = ['X5','X0','X8','X3','X1','X2','X314','X47','X118',\
'X315','X29','X127','X236','X115','X383','X152','X151','X351','X327','X77','X104',\
'X267','X95','X142']

train = h2o.import_file('/Users/kcrandall/GitHub/Kaggle_Mercedes_Manufacturing/data/train.csv')
test = h2o.import_file('/Users/kcrandall/GitHub/Kaggle_Mercedes_Manufacturing/data/test.csv')

# bug fix - from Keston
dummy_col = np.random.rand(test.shape[0])
test = test.cbind(h2o.H2OFrame(dummy_col))
cols = test.columns
cols[-1] = Y
test.columns = cols
print(train.shape)
print(test.shape)


original_nums, cats = get_type_lists(frame=train)


train, valid = train.split_frame([0.7], seed=12345)

total = len(cats)
for i, var in enumerate(cats):

    tr_enc, _ = target_encoder(train, test, var, Y)
    v_enc, ts_enc = target_encoder(valid, test, var, Y)

    print('Encoding: ' + var + ' (' + str(i+1) + '/' + str(total) + ') ...')

    train = train.cbind(tr_enc)
    valid = valid.cbind(v_enc)
    test = test.cbind(ts_enc)

print('Done.')

encoded_nums, cats = get_type_lists(frame=train)

#Remplace cats with encoded cats from MOST_IMPORTANT_VARS_ORDERD
for i, v in enumerate(MOST_IMPORTANT_VARS_ORDERD):
    if v in cats:
        MOST_IMPORTANT_VARS_ORDERD[i] = v + '_Tencode'


print('Combining features....')
(train, valid, test) = feature_combiner(train, test, MOST_IMPORTANT_VARS_ORDERD[0:12], valid_frame = valid, frame_type='h2o')
print('Done combining features.')

encoded_combined_nums, cats = get_type_lists(frame=train)

def ranked_preds_plot(y, valid, preds):

    """ Generates ranked prediction plot.

    :param y: Name of target variable.
    :param valid: Name of validation H2OFrame.
    :param preds: Column vector of predictions to plot.

    """

    # plot top frame values
    preds.columns = ['predict']
    yhat_frame = valid.cbind(preds)
    print(yhat_frame[0:10, [y, 'predict']])

    # plot sorted predictions
    yhat_frame_df = yhat_frame[[y, 'predict']].as_data_frame()
    yhat_frame_df.sort_values(by='predict', inplace=True)
    yhat_frame_df.reset_index(inplace=True, drop=True)
    _ = yhat_frame_df.plot(title='Ranked Predictions Plot')

import re
import time

def gen_submission(preds, test=test):

    """ Generates submission file for Kaggle House Prices contest.

    :param preds: Column vector of predictions.
    :param test: Test data.

    """

    # create time stamp
    time_stamp = re.sub('[: ]', '_', time.asctime())

    # create predictions column
    sub = test[ID_VAR].cbind(preds)
    sub.columns = [ID_VAR, Y]

    # save file for submission
    sub_fname = 'submission_' + str(time_stamp) + '.csv'
    h2o.download_csv(sub, sub_fname)


import os

def pred_blender(dir_, files):

    """ Performs simple blending of prediction files.

    :param dir_: Directory in which files to be read are stored.
    :param files: List of prediction files to be blended.

    """

    # read predictions in files list and cbind
    for i, file in enumerate(files):
        if i == 0:
            df = pd.read_csv(dir_ + os.sep + file).drop(Y, axis=1)
        col = pd.read_csv(dir_ + os.sep + file).drop(ID_VAR, axis=1)
        col.columns = [Y + str(i)]
        df = pd.concat([df, col], axis=1)

    # create mean prediction
    df['mean'] = df.iloc[:, 1:].mean(axis=1)
    print(df.head())

    # create time stamp
    time_stamp = re.sub('[: ]', '_', time.asctime())

    # write new submission file
    df = df[[ID_VAR, 'mean']]
    df.columns = [ID_VAR,Y]

    # save file for submission
    sub_fname = '../data/submission_' + str(time_stamp) + '.csv'
    df.to_csv(sub_fname, index=False)

# initialize rf model
rf_model1 = H2ORandomForestEstimator(
    ntrees=10000,
    max_depth=10,
    col_sample_rate_per_tree=0.1,
    sample_rate=0.8,
    stopping_rounds=50,
    score_each_iteration=True,
    nfolds=3,
    keep_cross_validation_predictions=True,
    seed=12345)

# train rf model
rf_model1.train(
    x=encoded_combined_nums,
    y=Y,
    training_frame=train,
    validation_frame=valid)

# print model information
print(rf_model1)

rf_preds1_val = rf_model1.predict(valid)
ranked_preds_plot(Y, valid, rf_preds1_val) # valid RMSE not so hot ...
rf_preds1_test = rf_model1.predict(test)
gen_submission(rf_preds1_test)


# initialize extra trees model
ert_model1 = H2ORandomForestEstimator(
    ntrees=10000,
    max_depth=10,
    col_sample_rate_per_tree=0.1,
    sample_rate=0.8,
    stopping_rounds=50,
    score_each_iteration=True,
    nfolds=3,
    keep_cross_validation_predictions=True,
    seed=12345,
    histogram_type='random')

# train ert model
ert_model1.train(
    x=encoded_combined_nums,
    y=Y,
    training_frame=train,
    validation_frame=valid)

# print model information/create submission
print(ert_model1)
ert_preds1_val = ert_model1.predict(valid)
ranked_preds_plot(Y, valid, ert_preds1_val) # valid RMSE not so hot ...
ert_preds1_test = ert_model1.predict(test)
gen_submission(ert_preds1_test)

# initialize H2O GBM
h2o_gbm_model = H2OGradientBoostingEstimator(
    ntrees = 10000,
    learn_rate = 0.005,
    sample_rate = 0.1,
    col_sample_rate = 0.8,
    max_depth = 5,
    nfolds = 3,
    keep_cross_validation_predictions=True,
    stopping_rounds = 10,
    seed = 12345)

# execute training
h2o_gbm_model.train(x=encoded_combined_nums,
                    y=Y,
                    training_frame=train,
                    validation_frame=valid)

# print model information/create submission
print(h2o_gbm_model)
h2o_gbm_preds1_val = h2o_gbm_model.predict(valid)
ranked_preds_plot(Y, valid, h2o_gbm_preds1_val) # better validation error
h2o_gbm_preds1_test = h2o_gbm_model.predict(test)
gen_submission(h2o_gbm_preds1_test)


# initialize XGB GBM
h2o_xgb_model = H2OXGBoostEstimator(
    ntrees = 10000,
    learn_rate = 0.005,
    sample_rate = 0.1,
    col_sample_rate = 0.8,
    max_depth = 5,
    nfolds = 3,
    keep_cross_validation_predictions=True,
    stopping_rounds = 10,
    seed = 12345)

# execute training
h2o_xgb_model.train(x=encoded_combined_nums,
                    y=Y,
                    training_frame=train,
                    validation_frame=valid)

# print model information/create submission
print(h2o_xgb_model)
h2o_xgb_preds1_val = h2o_xgb_model.predict(valid)
ranked_preds_plot(Y, valid, h2o_xgb_preds1_val)
h2o_xgb_preds1_test = h2o_xgb_model.predict(test)
gen_submission(h2o_xgb_preds1_test) 


# create XGBoost blend
# pred_blender('../data',
#             ['submission_Thu_Jun_15_15_58_31_2017.csv',
#              'submission_Thu_Jun_15_16_01_59_2017.csv',
#              'submission_Thu_Jun_15_16_27_42_2017.csv',
#              'submission_Thu_Jun_15_17_07_26_2017.csv'])


stack = H2OStackedEnsembleEstimator(training_frame=train,
                                    validation_frame=valid,
                                    base_models=[rf_model1, ert_model1,
                                                 h2o_gbm_model])

stack.train(x=encoded_combined_nums,
            y=Y,
            training_frame=train,
            validation_frame=valid)

# print model information/create submission
print(stack)
stack_preds1_val = stack.predict(valid)
ranked_preds_plot(Y, valid, stack_preds1_val)
stack_preds1_test = stack.predict(test)
gen_submission(stack_preds1_test)
