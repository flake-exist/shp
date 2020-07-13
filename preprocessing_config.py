import numpy as np

#Aggregated format file necessarycolumns

USER_PATH = "user_path"
COUNT     = "count"
#Aggregated format file necessary columns

#Detailed format file necessarycolumns

CLIENT_ID = "ClientID"
TIMELINE = "timeline"
#USER_PATH = "user_path" already announced in Aggregated format neccesary columns
#Detailed format file necessarycolumns


CHANNEL_DELIMITER = "=>"
TIMELINE_DELIMITER = "=>"

CHANNEL_NAME  = "channel_name"
SHAPLEY_VALUE = "shapley_value"
POSITION      = "position"

NECESSARY_COLS = [USER_PATH,COUNT]
D_NECESSARY_COLS = [CLIENT_ID,USER_PATH,TIMELINE]


NECESSARY_COLTYPES = {USER_PATH:np.object,COUNT:np.int}
D_NECESSARY_COLTYPES = {CLIENT_ID:np.object,USER_PATH:np.object,TIMELINE:np.object}


TIME_ZONE=3
MILISEC_FORMAT=True
THOUSAND=1000
MIN_PER_HOUR = 60
SEC_PER_MIN = 60

DAY_PATTERN = r'^\d{1,10}[Dd]$'
DAY_WINDOW = r'^(\d{1,10})[Dd]$'

MONTH_PATTERN = r'^\d{1,10}MS$'
MONTH_WINDOW = r'^(\d{1,10})[Mm]$'

SEPARATOR=","

NUMAFTERPOINT = 5
ERROR = 0.01

DATE_START = "date_start"
DATE_FINISH = "date_finish"

def ChainSplit(chain,channel_delimiter): return chain.split(channel_delimiter)

def EncodeDict(seq):
    '''
    Create `self.encryped_dict` indicating each unique channel its unique ID
    Format -> {channel_1 : ID_1,
                 ...
                 ...
                 ...
               channel_n : ID_n}

    INPUT: [1] `channel_store` list with unique channels
    RETURN: [1] Unit
    '''
    
    seq_unique = set(seq)
    elem_dict = {}

    for ID,elem in enumerate(seq_unique):
        elem_dict[elem] = ID

    return elem_dict


def ElemEncode(seq_toEncode,encryped_dict): 
    '''
    Convert channel[s] into ID[s]
    INPURT: [1] `channel_seq` list of channels
    RETURN: [1] list of IDs (List[Int])
        '''
    return [encryped_dict[channel] for channel in seq_toEncode]


def DecodeDict(encoded_dict,encryped_dict):
    '''
    Decode `shapley_dict`, where each unique channel is incoded into unique ID to "human" string format
    Before: -> {0:val1,
                1:val2,
                ...,
                ...,
                ...,
                N: val_N} where N - number of unique channels and val_N its values (weights)

    After-> {channel_name_1:val1,
             channel_name_2:val2,
                ...,
                ...,
                ...,
             channel_name_N: val_N} where N - number of unique channels and val_N its values (weights)

    '''

    decode_dict = {}

    inv_channel_dict = {v: k for k, v in encryped_dict.items()} # create invert `encryped_dict

    for key in encoded_dict.keys():
        decode_dict[inv_channel_dict[key]] = encoded_dict[key]

    return decode_dict


def ZeroMatrix(n ,m): 
    '''
    Create zero matrix (size = (number of unique chains) x (number of unique channels))
    '''

    return np.zeros((n,m))

def FilterTheDict(dictObj,callback):
    newDict = dict()
    for (key, value) in dictObj.items():
        if callback((key, value)):
            newDict[key] = value
    return newDict

def epochCombinator(seq):
    index = 0
    store = []
    while index < len(seq) - 1:
        row = (seq[index],seq[index+1])
        store.append(row)
        index += 1
    return store
