# imports
import pandas as pd
import numpy as np



def feature_combiner(training_frame, test_frame, nums, frame_type='h2o'):

    """ Combines numeric features using simple arithmatic operations.

    :param training_frame: Training frame from which to generate features and onto which generated
                           feeatures will be cbound.
    :param test_frame: Test frame from which to generate features and onto which generated
                       feeatures will be cbound.
    :param nums: List of original numeric features from which to generate combined features.
    :param frame_type: The type of frame that is input and output. Accepted: 'h2o', 'pandas'

    """

    total = len(nums)

    if frame_type == 'spark':

        train_df = training_frame
        test_df = test_frame

        for i, col_i in enumerate(nums):
            print('Combining: ' + col_i + ' (' + str(i+1) + '/' + str(total) + ') ...')

            for j, col_j in enumerate(nums):

                # don't repeat (i*j = j*i)
                if i < j:

                    # multiply, add a new column
                    train_df = train_df.withColumn(str(col_i + '|' + col_j), train_df[col_i]*train_df[col_j])
                    test_df = test_df.withColumn(str(col_i + '|' + col_j), test_df[col_i]*test_df[col_j])
        return train_df, test_df

        print('DONE combining features.')
    else:
        train_df, test_df = None, None
        if frame_type == 'h2o':
            # convert to pandas
            train_df = training_frame.as_data_frame()
            test_df = test_frame.as_data_frame()
        elif frame_type == 'pandas':
            train_df = training_frame
            test_df = test_frame

        for i, col_i in enumerate(nums):

            print('Combining: ' + col_i + ' (' + str(i+1) + '/' + str(total) + ') ...')

            for j, col_j in enumerate(nums):

                # don't repeat (i*j = j*i)
                if i < j:

                    # convert to pandas
                    col_i_train_df = train_df[col_i]
                    col_j_train_df = train_df[col_j]
                    col_i_test_df = test_df[col_i]
                    col_j_test_df = test_df[col_j]

                    # multiply, convert back to h2o
                    train_df[str(col_i + '|' + col_j)] = col_i_train_df.values*col_j_train_df.values
                    test_df[str(col_i + '|' + col_j)] = col_i_test_df.values*col_j_test_df.values

        print('DONE combining features.')


        if frame_type == 'pandas':
            return train_df, test_df
        elif frame_type == 'h2o':
            # convert back to h2o
            import h2o
            print('Converting to H2OFrame ...')
            # convert train back to h2o
            training_frame = h2o.H2OFrame(train_df)
            training_frame.columns = list(train_df)
            # conserve memory
            del train_df
            # convert test back to h2o
            test_frame = h2o.H2OFrame(test_df)
            test_frame.columns = list(test_df)
            # conserve memory
            del test_df

            print('Done.')

            return training_frame, test_frame
