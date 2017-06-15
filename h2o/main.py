# imports
import pandas as pd # import pandas for easy data manipulation using data frames
import numpy as np  # import numpy for numeric calculations on matrices
import time         # for timers
import os

# import h2o to check calculations
import h2o
from h2o.estimators.glm import H2OGeneralizedLinearEstimator # import GLM models
from h2o.grid.grid_search import H2OGridSearch               # grid search
# start h2o
h2o.init() #max_mem_size='12G'
# h2o.remove_all()
h2o.show_progress()                                          # turn on progress bars


from feature_combiner import feature_combiner
from target_encoder import target_encoder
from get_type_lists import get_type_lists

# data-related constants
IN_FILE_PATH = os.path.dirname( __file__ ) + '/data/train.csv'
Y            = 'y'
ID_VAR       = 'ID'
DROPS        = [ID_VAR]
#KAGGLE UNLABELED TEST DATA
IN_FILE_PATH_TEST = os.path.dirname( __file__ ) + '/data/test.csv'
test = h2o.import_file(IN_FILE_PATH_TEST)

# model-related constants
LEARN_RATE   = 0.005 # how much each gradient descent step impacts parameters
CONV         = 1e-10 # desired precision in parameters
MAX_ITERS    = 10000 # maximum number of gradient descent steps to allow

# numeric columns
train = h2o.import_file(IN_FILE_PATH)
train = train.drop(DROPS)
X = train.col_names

# train.describe()
# exit()

original_numerics, categoricals = get_type_lists(frame=train,rejects=[ID_VAR,Y]) #These three have test varaibles that don't occur in the train dataset

print("Encoding numberic variables...")
for i, var in enumerate(categoricals):
    total = len(categoricals)

    print('Encoding: ' + var + ' (' + str(i+1) + '/' + str(total) + ') ...')

    tr_enc, ts_enc = target_encoder(train, test, var, Y)

    train = train.cbind(tr_enc)
    test = test.cbind(ts_enc)

print('Done.')

# run again after encoding
encoded_numerics, categoricals = get_type_lists(frame=train,rejects=[ID_VAR,Y,'X2','X0','X5'])

# create interaction variables
train, test = feature_combiner(train, test, encoded_numerics)

# run again after interactions
encoded_combined_numerics, categoricals = get_type_lists(frame=train,rejects=[ID_VAR,Y,'X2','X0','X5'])


# check number of created variables is correct
# 1 id column, 1 target column, 79 original + encoded numeric columns, 43 original categorical variables
# sum(range(1, 79)) combined variables
print(train.shape == (1460, sum(range(1, 79), (79 + 43 + 1 + 1))))
print(test.shape == (1459, sum(range(1, 79), (79 + 43 + 1))))

# split data
print('Splitting Data...')
base_train, base_valid, stack_train, stack_valid = train.split_frame([0.3, 0.2, 0.3], seed=654251)
print(base_train.shape)
print(base_valid.shape)
print(stack_train.shape)
print(stack_valid.shape)
print('Data split.')


def glm_grid(X, y, train, valid):

    """ Wrapper function for penalized GLM with alpha and lambda search.

    :param X: List of inputs.
    :param y: Name of target variable.
    :param train: Name of training H2OFrame.
    :param valid: Name of validation H2OFrame.
    :return: Best H2Omodel from H2OGeneralizedLinearEstimator

    """

    alpha_opts = [0.01, 0.25, 0.5, 0.99] # always keep some L2
    hyper_parameters = {"alpha":alpha_opts}

    # initialize grid search
    grid = H2OGridSearch(
        H2OGeneralizedLinearEstimator(
            family="gaussian",
            lambda_search=True,
            seed=12345),
        hyper_params=hyper_parameters)

    # train grid
    grid.train(y=y,
               x=X,
               training_frame=train,
               validation_frame=valid)

    # show grid search results
    print(grid.show())

    best = grid.get_grid()[0]
    print(best)

    # plot top frame values
    yhat_frame = valid.cbind(best.predict(valid))
    print(yhat_frame[0:10, [y, 'predict']])

    # plot sorted predictions
    yhat_frame_df = yhat_frame[[y, 'predict']].as_data_frame()
    yhat_frame_df.sort_values(by='predict', inplace=True)
    yhat_frame_df.reset_index(inplace=True, drop=True)
    _ = yhat_frame_df.plot(title='Ranked Predictions Plot')

    # select best model
    return best


# TRAIN all the models
glm0 = glm_grid(original_numerics, Y, base_train, base_valid)
glm1 = glm_grid(encoded_numerics, Y, base_train, base_valid)
glm2 = glm_grid(encoded_combined_numerics, Y, base_train, base_valid)
print('Model training done.')


stack_train = stack_train.cbind(glm0.predict(stack_train))
stack_valid = stack_valid.cbind(glm0.predict(stack_valid))
stack_train = stack_train.cbind(glm1.predict(stack_train))
stack_valid = stack_valid.cbind(glm1.predict(stack_valid))
stack_train = stack_train.cbind(glm2.predict(stack_train))
stack_valid = stack_valid.cbind(glm2.predict(stack_valid))

test = test.cbind(glm0.predict(test))
test = test.cbind(glm1.predict(test))
test = test.cbind(glm2.predict(test))

glm_stack_model = glm_grid(encoded_combined_numerics + ['predict', 'predict0', 'predict1'], Y, stack_train, stack_valid)


# Score test data
sub = test[ID_VAR].cbind(glm_stack_model.predict(test))
sub['predict'] = sub['predict'].exp()
print(sub.head())

# create time stamp
import re
import time
time_stamp = re.sub('[: ]', '_', time.asctime())

# save file for submission
sub.columns = [ID_VAR, Y]
sub_fname = os.path.dirname( __file__ )+ '/data/submissions/submission_' + str(time_stamp) + '.csv'
h2o.download_csv(sub, sub_fname)

# shutdown h2o
# h2o.cluster().shutdown(prompt=False)
