import pandas as pd
from preprocessing_config import *

class Preprocessing:
    
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

        if all([elem in cols for elem in necessary_cols]):
            return True
        else:
            raise ValueError("`{0}` must contain {1}".format(cols,necessary_cols))
            
    #columnType
    def columnType(self):
        '''
        Check columns correct types
        RETURN : [1] Boolean
        '''

        col_Types = dict(self.data.dtypes)
        for key in col_Types.keys():
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
            print("Not Empty Data             : +")
        if self.columnAvailaibility():
            print("Column availaibility       : +")
        if self.columnType():
            print("Column Type correctness    : +")
        if self.uniqueChain():
            print("Unique Chain availaibility : +")
            
        return True
    
if __name__ == '__main__':
    data = pd.read_csv("path_to_file")
    r = Preprocessing(data).run()