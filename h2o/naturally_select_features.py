from dimensionality_reduction import dimensionality_reduction


def naturally_select_features(train, valid, X, Y, frame_type='h2o'):
    """

    """


    train_frame = train.as_data_frame()

    rounds_till_death = 3
    number_of_features_to_make_the_cut = 0.2
    n_folds = 20

    features_made_the_cut = []

    features_passing_to_next_round = []
    features_passed_previous_round = []

    if frame_type == 'spark':
        pass
    else:
        import h2o
        import math
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split
        from sklearn.model_selection import KFold
        from h2o.estimators.xgboost import H2OXGBoostEstimator

        for i in range(0,200):
            if i==0:
                # print(train_frame.columns)
                # print(X)
                features_passed_previous_round = X
                features_passing_to_next_round = []
            else:
                # Rest lists
                features_passed_previous_round = features_passing_to_next_round
                features_passing_to_next_round = []

            for j in range(1,3):
                column_frames = []
                kf = KFold(n_splits=n_folds)
                for train_index, test_index in kf.split(features_passed_previous_round):
                    # print(test_index)
                    columns = []
                    for x in test_index:
                        columns.append(features_passed_previous_round[x])
                    column_frames.append(columns)
                # for k in range(0,n_folds):
                #     df_1, df_2 = train_test_split(df, test_size = 0.2)
                #     frames.append(df_1)
                #     frames.append(df_2)
                for k in range(0,n_folds):
                    # Train an XGBoost model to select the most imporant vars
                    #
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
                    h2o_xgb_model.train(x=column_frames[k],
                                        y=Y,
                                        training_frame=train,
                                        validation_frame=valid)
                    most_important_variables = h2o_xgb_model.varimp(use_pandas=False)

                    number_of_features_passed = math.floor(len(train_frame.columns)*number_of_features_to_make_the_cut)
                    features_passing_to_next_round.append(most_important_variables[0:number_of_features_passed-1])
                    print(most_important_variables[0:number_of_features_passed-1])
                    print(features_passing_to_next_round)
