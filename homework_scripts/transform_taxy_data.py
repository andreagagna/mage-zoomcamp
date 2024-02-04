if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re

def camel_to_snake_case(input_list):
    snake_case_list = []
    for item in input_list:
        # Use regular expression to find uppercase letters and insert underscore before them
        snake_case_item = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', item).lower()
        snake_case_list.append(snake_case_item)
    return snake_case_list


@transformer
def transform(data, *args, **kwargs):
    # remove trips with no passengers and no distance
    filter_mask = (data['passenger_count'] != 0) & (data['trip_distance'] != 0)
    print("Rows with zero passangers and zero distance:",
          (filter_mask.sum()))
    data = data.loc[filter_mask]

    # create date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # camel to snake case
    new_cols = camel_to_snake_case(list(data.columns))
    changed_cols = set(data.columns).difference(set(new_cols))
    print('Changed columns:', changed_cols)
    data.columns = new_cols

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert (sorted(output['vendor_id'].dropna().unique( ).tolist())== [1, 2]), 'There are anomalous vendor IDs.'
    assert (output['passenger_count'] == 0).sum() == 0, 'There are rides with zero passengers.'
    assert (output['trip_distance'] == 0).sum() == 0, 'There are rides with zero distance.'
