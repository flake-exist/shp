import pandas as pd
from preprocessing_config import *

class Preprocessing_agg:
    
    def __init__(self,data):
        self.data = data
        
    def notEmpty(self):
        if self.data.shape[0] != 0:
            return True
        else:
            raise ValueError("DataFrame is Empty")
    
    #columnAvailaibility
    def columnAvailaibility(self):
        '''
        Check availaibility all of necessary columns
        RETURN
        '''

        cols = list(self.data.columns)
        
        necessary_cols = NECESSARY_COLS
                  
        if all([elem in cols for elem in necessary_cols]):
            return True
        else:
            raise ValueError("Find columns: {0}\n Required columns : {1}".format(cols,necessary_cols))
            
    #columnType
    def columnType(self):
        '''
        Check columns correct types
        RETURN : [1] Boolean
        '''
        
        col_Types = dict(self.data.dtypes)

        necessary_colTypes = NECESSARY_COLTYPES
            
        for key in necessary_colTypes.keys():
            if col_Types[key] == necessary_colTypes[key]:
                True
            else:
                raise TypeError("Find : {0}[{1}]\nRequired : {0}[{2}]".format(key,col_Types[key],necessary_colTypes[key]))
        return True
    
    #uniqueChain
    def uniqueChain(self):
        '''
        Check if `user_path` contains only unique paths or not
        RETURN : [1] Boolean
        '''
        
        path_count = self.data[USER_PATH].shape[0]
        unique_count = self.data[USER_PATH].unique().shape[0]

        if path_count == unique_count:
            return True       
        else:
            raise ValueError("`{0}` must contain only unique (aggregated) paths".format(USER_PATH))
    
    def run(self):
        
        if self.notEmpty():
            print("Not Empty DataFrame        : +")
        if self.columnAvailaibility():
            print("Column availaibility       : +")
        if self.columnType():
            print("Column Type correctness    : +")
        if self.uniqueChain():
            print("Unique Chain availaibility : +")
            
        return True
    
# if __name__ == '__main__':
#     data = pd.read_csv("path_to_file")
#     r = Preprocessing(data).run()