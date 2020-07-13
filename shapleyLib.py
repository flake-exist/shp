import argparse
import pandas as pd
import numpy as np
from preprocessing_agg import Preprocessing_agg
from preprocessing_config import *
from properties import Properties
from shapleyOrderLib import shapleyOrderLib
import os


class shapleyLib:
    
    def __init__(self,data,path_col=USER_PATH,count_col=COUNT,channel_delimiter=CHANNEL_DELIMITER):
        
        self.data              = data  
        self.path_col          = path_col
        self.count_col         = count_col
        self.channel_delimiter = channel_delimiter   
        self.encryped_dict      = {} #dict for incoding channel names

    
    def PathStats(self):
        '''
        Count number of unique channels in `paths`
        RETURN : [1] Unit
        '''
        
        channel_store = []
        
        path_count = self.data[self.path_col].shape[0] #number of paths
        
        for chain in self.data[self.path_col].values:
            channel_store.extend(ChainSplit(chain,self.channel_delimiter)) #UDF method
        channel_unique = set(channel_store) #unique channels
        
        channel_unique_count = len(channel_unique) #number of unique channles
        
        self.encryped_dict = EncodeDict(channel_unique)#UDF method   
        
        return path_count,channel_unique_count
        
    
    def Vectorization(self):
        '''
        Convert from `chain_list` format to Matrix format.
        Each ID in matrix is allocated to a channel (see`self.EncodeDict`)
        RETURN: [1] Matrix[Int]. If i-th chain(row) has j-th channel(ID) -> M(i,j) = 1.In case channel(ID) meets multiple
        times in chain , M ignoring it anyway , despite of frequency channel(ID) in chain M(i,j)=1
        '''
        
        path_count,channel_unique_count = self.PathStats()
        
        M = ZeroMatrix(path_count,channel_unique_count) #create empty Matrix (n x m)
        for index,chain in enumerate(self.data[self.path_col].values):
            channel_seq = ChainSplit(chain,self.channel_delimiter)
            channel_seq_id = ElemEncode(channel_seq,self.encryped_dict)
            M[index,channel_seq_id] = 1
            
        return M
            
    def Calc(self,M):
        
        shapley_DictEncode = {}
        shapley_DictDecode = {}
#         decode_dict={}
        
        for i in range(M.shape[1]): # iterate by channels
            mask = M[:,i]>0 # create mask
            M_buffer = M[mask] # rows contiaining i-th channel
            channel_cardinality = M_buffer.sum(axis=1) #channel cardinality           
            conversion = self.data[self.count_col].values[mask] # conversions allocated to i-th channel
            result = np.stack((conversion, channel_cardinality), axis=-1) #
            shapley_DictEncode[i] = np.array(result[:,0]/result[:,1]).sum() # 
            
        shapley_DictDecode = DecodeDict(shapley_DictEncode,self.encryped_dict)
        
        return shapley_DictEncode,shapley_DictDecode
    
    
    def run(self,output_filename=None,output_filename_order=None):
        
        Preprocessing_agg(self.data).run()
        
        M = self.Vectorization()

        shapley_DictEncode,shapley_DictDecode = self.Calc(M)

        properties = Properties(self.data,shapley_DictEncode)

        if properties.Efficiency():
            print("Properties / Efficience  : +")
        if properties.DummyPlayer(M):
            print("Properties / DummyPlayer : +")

        shapley_calc = pd.DataFrame(shapley_DictDecode.items(),columns=[CHANNEL_NAME,SHAPLEY_VALUE])

        shapley_calcOrder = shapleyOrderLib(self.data).run(M,shapley_DictDecode,self.encryped_dict)

        record_paths = [output_filename,output_filename_order]

        returnORwrite = all([(val==None) for val in record_paths])

        if returnORwrite == True:
            return shapley_calc,shapley_calcOrder
        else:
            shapley_calc.to_csv(output_filename)
            shapley_calcOrder.to_csv(output_filename_order)
                
    
if __name__ == '__main__':
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--input_filepath', action='store', type=str, required=True)                                
    my_parser.add_argument('--output_filename', action='store', type=str, required=True)
    my_parser.add_argument('--output_filename_order', action='store', type=str, required=True)
    args = my_parser.parse_args()
    
    data = pd.read_csv(args.input_filepath)
    
    shapley = shapleyLib(data)
    shapley.run(args.output_filename,args.output_filename_order)                                 