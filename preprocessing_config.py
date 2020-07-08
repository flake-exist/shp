import numpy as np

USER_PATH = "user_path"
COUNT     = "count"
CHANNEL_DELIMITER = "=>"

CHANNEL_NAME = "channel_name"
SHAPLEY_VALUE = "shapley_value"

necessary_cols = [USER_PATH,COUNT]
necessary_colTypes = {"user_path":np.object,"count":np.int}