# Great Expectations Suites (Exported)

These suites are generated from code-first definitions in `test/data_validation/gx_suites.py`.

## rb_market_data_suite

- Total expectations: **23**

- **expect_table_columns_to_contain_set** `{}`
- **expect_column_values_to_match_regex** `{'column': 'timestamp'}`
- **expect_column_values_to_not_be_null** `{'column': 'open', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'high', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'low', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'close', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'volume', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'VPIN', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'vol', 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'open', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'high', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'low', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'close', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'volume', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'VPIN', 'min_value': 0.0, 'max_value': 1.0, 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'vol', 'min_value': 0.0, 'max_value': None, 'mostly': 0.999}`
- **expect_column_pair_values_A_to_be_greater_than_or_equal_to_B** `{'column_A': 'high', 'column_B': 'low', 'mostly': 0.999, 'row_condition': "event_type == 'TICK'"}`
- **expect_column_pair_values_A_to_be_greater_than_or_equal_to_B** `{'column_A': 'high', 'column_B': 'open', 'mostly': 0.995, 'row_condition': "event_type == 'TICK'"}`
- **expect_column_pair_values_A_to_be_greater_than_or_equal_to_B** `{'column_A': 'high', 'column_B': 'close', 'mostly': 0.995, 'row_condition': "event_type == 'TICK'"}`
- **expect_column_pair_values_A_to_be_less_than_or_equal_to_B** `{'column_A': 'low', 'column_B': 'open', 'mostly': 0.995, 'row_condition': "event_type == 'TICK'"}`
- **expect_column_pair_values_A_to_be_less_than_or_equal_to_B** `{'column_A': 'low', 'column_B': 'close', 'mostly': 0.995, 'row_condition': "event_type == 'TICK'"}`
- **expect_column_proportion_of_unique_values_to_be_between** `{'column': 'VPIN', 'min_value': 0.001, 'max_value': 1.0}`
- **expect_column_values_to_not_be_null** `{'column': 'vol', 'mostly': 0.999}`

## rb_sentiment_data_suite

- Total expectations: **12**

- **expect_table_columns_to_contain_set** `{}`
- **expect_column_values_to_not_be_null** `{'column': 'timestamp', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'ticker', 'mostly': 0.999}`
- **expect_column_values_to_be_between** `{'column': 'sentiment_score', 'min_value': -1.0, 'max_value': 1.0, 'mostly': 0.999}`
- **expect_column_values_to_be_in_set** `{'column': 'sentiment_label', 'mostly': 0.999}`
- **expect_column_values_to_not_be_null** `{'column': 'headline', 'mostly': 0.999}`
- **expect_column_value_lengths_to_be_between** `{'column': 'headline', 'min_value': 5, 'max_value': 500, 'mostly': 0.98}`
- **expect_column_values_to_not_be_null** `{'column': 'url', 'mostly': 0.95}`
- **expect_column_values_to_match_regex** `{'column': 'url', 'mostly': 0.95}`
- **expect_column_values_to_be_between** `{'column': 'sentiment_score', 'min_value': 0.0, 'max_value': 1.0, 'mostly': 0.9, 'row_condition': "sentiment_label == 'positive'"}`
- **expect_column_values_to_be_between** `{'column': 'sentiment_score', 'min_value': -1.0, 'max_value': 0.0, 'mostly': 0.9, 'row_condition': "sentiment_label == 'negative'"}`
- **expect_column_values_to_be_between** `{'column': 'sentiment_score', 'min_value': -0.2, 'max_value': 0.2, 'mostly': 0.8, 'row_condition': "sentiment_label == 'neutral'"}`
