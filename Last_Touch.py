"""
filepath - папка исходного файлф
filename - имя исходного файла
output_filepath - папка куда сохранять 
output_filename - если название нового файла нужно изменять

запускается функцией run_class(filepath,filename,output_filepath,output_filename)
"""


import pandas as pd
import numpy as np
import csv


def run_class(filepath,filename,output_filepath,output_filename, sep):
    LT=LastTouch(filepath,filename,output_filepath,output_filename, sep='=>')
    res=LT.get_result()
    return res

class LastTouch:
    def __init__(self,filepath='', filename='',output_filepath='', output_filename='', sep='=>'):
        self.filepath=filepath
        self.filename=filename
        self.output_filepath=output_filepath
        self.output_filename=output_filename
        self.sep=sep
        self.chain_data=None
        
        
    def open_csv_with_clid(self, path=''):
        with open(path) as f:
            reader = csv.reader(f)
            listrow=[]
            for row in reader:
                listrow.append(row)
        return pd.DataFrame(listrow[1:], columns=listrow[0])        
        
    def get_chan_list(self):
        return list(set([y for i in list(self.chain_data.user_path) for y in i.split(self.sep)]))
    
    def get_last_touch(self, unique_touch_list):
        """
        calc last touch conversion 
        """
        conv_dict_2 = dict.fromkeys(unique_touch_list,0)
        for inx in self.chain_data.values:
            path_list = inx[0].split(self.sep)
            conv_dict_2[path_list[-1]] += int(inx[1])
        return conv_dict_2
    
    def make_result_df(self, dict_lasttouch):
        df=pd.DataFrame.from_dict(dict_lasttouch, orient='index')
        df.reset_index(inplace=True)
        df.columns=['channel_name', 'value']
        df['variable']='last_touch'
        return df
    
    def safe_result(self,res_data=None ):
        res_data.to_csv(self.output_filepath+self.output_filename+'csv',sep=',', index=False, float_format='%.100f')
        return 
    
    def get_result(self,):
        self.chain_data=self.open_csv_with_clid(path=self.filepath+self.filename)
        ch_name=self.get_chan_list()
        lt_res=self.get_last_touch(ch_name)
        res=self.make_result_df(lt_res)
        self.safe_result(res_data=res )
        return res
    
if __name__ == '__main__':
    run_class(filepath,filename,output_filepath,output_filename,sep)