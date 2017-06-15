def get_type_lists(frame, rejects=['Id', 'ID','id']):

    """Creates lists of numeric and categorical variables.

    :param frame: The frame from which to determine types.
    :param rejects: Variable names not to be included in returned lists.
    :return: Tuple of lists for numeric and categorical variables in the frame.

    """

    nums, cats = [], []
    for key, val in frame.types.items():
        if key not in rejects:
            if val == 'enum':
                cats.append(key)
            else:
                nums.append(key)

    print('Numeric =', nums)
    print()
    print('Categorical =', cats)

    return nums, cats
