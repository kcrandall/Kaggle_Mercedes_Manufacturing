def dimensionality_reduction(train_frame,test_frame,columns,n_comp=320,random_seed=420,decompositions_to_run=['PCA','TSVD','ICA','GRP','SRP'],valid_frame=None,frame_type='h2o',test_does_have_y=False):
    """
    Shrink input features in n_comp features using one or more decomposition functions.
    h2o/pandas frames supports: ['PCA','TSVD','ICA','GRP','SRP']
    spark frame supports: NOT IMPLEMENTED TODO
    :param train_frame: an input frame of the training data
    :param test_frame: an input frame of the test data
    :param n_comp: the nuimber of features you want return (per teqnique)
    :param random_seed: the random seed you want to make the decompositions with
    :param valid_frame: (optional) an input frame with validation data
    :param frame_type: the frame type you want input and returned
    :param test_does_have_y: if the test has y values. If it does then it will caculate an independent vector to prevent feature leakage
    :return: Either tuple of (train_frame,test_frame) or (train_frame,valid_frame,test_frame)
    """
    if frame_type == 'spark':
        pass
    elif frame_type in ['h2o','pandas']:
        from sklearn.random_projection import GaussianRandomProjection
        from sklearn.random_projection import SparseRandomProjection
        from sklearn.decomposition import PCA, FastICA
        from sklearn.decomposition import TruncatedSVD

        train_df, test_df, valid_df = None, None, None
        if frame_type == 'h2o':
            # convert to pandas
            train_df = train_frame.as_data_frame()
            test_df = test_frame.as_data_frame()
            valid_df = valid_frame.as_data_frame()
        elif frame_type == 'pandas':
            train_df = training_frame
            test_df = test_frame
            valid_df = valid_frame

        train_df = train_df[columns]
        if valid_frame:
            valid_df = valid_df[columns]
        test_df = test_df[columns]


        tsvd_results_train, tsvd_results_valid, tsvd_results_test = None, None, None
        if 'TSVD' in decompositions_to_run:
            tsvd = TruncatedSVD(n_components=n_comp, random_state=random_seed)
            tsvd_results_train = tsvd.fit_transform(train_df)
            tsvd_results_valid, tsvd_results_test = None, None
            if valid_frame:
                tsvd2 = TruncatedSVD(n_components=n_comp, random_state=random_seed)
                tsvd_results_valid = tsvd2.fit_transform(valid_df)
                if test_does_have_y:
                    tsvd3 = TruncatedSVD(n_components=n_comp, random_state=random_seed)
                    tsvd_results_test = tsvd3.fit_transform(test_df)
                else:
                    tsvd_results_test = tsvd2.transform(test_df)
            else:
                if test_does_have_y:
                    tsvd3 = TruncatedSVD(n_components=n_comp, random_state=random_seed)
                    tsvd_results_test = tsvd3.fit_transform(test_df)
                else:
                    tsvd_results_test = tsvd.transform(test_df)

        #PCA
        pca_results_train, pca_results_valid, pca_results_test = None, None, None
        if 'PCA' in decompositions_to_run:
            pca = PCA(n_components=n_comp, random_state=random_seed)
            pca_results_train = pca.fit_transform(train_df)
            if valid_frame:
                pca2 = PCA(n_components=n_comp, random_state=random_seed)
                pca_results_valid = pca2.fit_transform(valid_df)
                if test_does_have_y:
                    pca3 = PCA(n_components=n_comp, random_state=random_seed)
                    pca_results_test = pca3.fit_transform(test_df)
                else:
                    pca_results_test = pca2.transform(test_df)
            else:
                if test_does_have_y:
                    pca3 = PCA(n_components=n_comp, random_state=random_seed)
                    pca_results_test = pca3.fit_transform(test_df)
                else:
                    pca_results_test = pca.transform(test_df)

        # ICA
        ica_results_train, ica_results_valid, ica_results_test = None, None, None
        if 'ICA' in decompositions_to_run:
            ica = FastICA(n_components=n_comp, random_state=random_seed)
            ica_results_train = ica.fit_transform(train_df)
            if valid_frame:
                ica2 = FastICA(n_components=n_comp, random_state=random_seed)
                ica_results_valid = ica2.fit_transform(valid_df)
                if test_does_have_y:
                    ica3 = FastICA(n_components=n_comp, random_state=random_seed)
                    ica_results_test = ica3.fit_transform(test_df)
                else:
                    ica_results_test = ica2.transform(test_df)
            else:
                if test_does_have_y:
                    ica3 = FastICA(n_components=n_comp, random_state=random_seed)
                    ica_results_test = ica3.fit_transform(test_df)
                else:
                    ica_results_test = ica.transform(test_df)


        # GRP
        grp_results_train, grp_results_valid, grp_results_test = None, None, None
        if 'GRP' in decompositions_to_run:
            grp = GaussianRandomProjection(n_components=n_comp, random_state=random_seed)
            grp_results_train = grp.fit_transform(train_df)
            if valid_frame:
                grp2 = GaussianRandomProjection(n_components=n_comp, random_state=random_seed)
                grp_results_valid = grp2.fit_transform(valid_df)
                if test_does_have_y:
                    grp3 = GaussianRandomProjection(n_components=n_comp, random_state=random_seed)
                    grp_results_test = grp3.fit_transform(test_df)
                else:
                    grp_results_test = grp2.transform(test_df)
            else:
                if test_does_have_y:
                    grp3 = GaussianRandomProjection(n_components=n_comp, random_state=random_seed)
                    grp_results_test = grp3.fit_transform(test_df)
                else:
                    grp_results_test = grp.transform(test_df)

        # SRP
        srp_results_train, srp_results_valid, srp_results_test = None, None, None
        if 'SRP' in decompositions_to_run:
            srp = SparseRandomProjection(n_components=n_comp, random_state=random_seed)
            srp_results_train = srp.fit_transform(train_df)
            if valid_frame:
                srp2 = SparseRandomProjection(n_components=n_comp, random_state=random_seed)
                srp_results_valid = srp2.fit_transform(valid_df)
                if test_does_have_y:
                    srp3 = SparseRandomProjection(n_components=n_comp, random_state=random_seed)
                    srp_results_test = srp3.fit_transform(test_df)
                else:
                    srp_results_test = srp2.transform(test_df)
            else:
                if test_does_have_y:
                    srp3 = SparseRandomProjection(n_components=n_comp, random_state=random_seed)
                    srp_results_test = srp3.fit_transform(test_df)
                else:
                    srp_results_test = srp.transform(test_df)

        for i in range(1, n_comp + 1):
            if 'PCA' in decompositions_to_run:
                train_df['pca_' + str(i)] = pca_results_train[:, i - 1]
                if valid_frame:
                    valid_df['pca_' + str(i)] = pca_results_valid[:, i - 1]
                test_df['pca_' + str(i)] = pca_results_test[:, i - 1]

            if 'ICA' in decompositions_to_run:
                train_df['ica_' + str(i)] = ica_results_train[:, i - 1]
                if valid_frame:
                    valid_df['pca_' + str(i)] = ica_results_valid[:, i - 1]
                test_df['ica_' + str(i)] = ica_results_test[:, i - 1]

            if 'TSVD' in decompositions_to_run:
                train_df['tsvd_' + str(i)] = tsvd_results_train[:, i - 1]
                if valid_frame:
                    valid_df['pca_' + str(i)] = tsvd_results_valid[:, i - 1]
                test_df['tsvd_' + str(i)] = tsvd_results_test[:, i - 1]

            if 'GRP' in decompositions_to_run:
                train_df['grp_' + str(i)] = grp_results_train[:, i - 1]
                if valid_frame:
                    valid_df['pca_' + str(i)] = grp_results_valid[:, i - 1]
                test_df['grp_' + str(i)] = grp_results_test[:, i - 1]

            if 'SRP' in decompositions_to_run:
                train_df['srp_' + str(i)] = srp_results_train[:, i - 1]
                if valid_frame:
                    valid_df['pca_' + str(i)] = srp_results_valid[:, i - 1]
                test_df['srp_' + str(i)] = srp_results_test[:, i - 1]

        if frame_type == 'pandas':
            if valid_frame:
                return (train_df, valid_df, test_df)
            else:
                return (train_df, test_df)
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
            validation_frame = None
            if valid_frame:
                # convert test back to h2o
                validation_frame = h2o.H2OFrame(valid_df)
                validation_frame.columns = list(valid_df)
                # conserve memory
                del valid_df

            print('Done.')

            if valid_frame:
                return training_frame, validation_frame, test_frame
            else:
                return training_frame, test_frame
