import argparse
import pandas as pd
import numpy as np
from preprocessing import Preprocessing
from preprocessing_config import *
from properties import Properties


class Shapley:
    
    def __init__(self,data,path_col=USER_PATH,count_col=COUNT,channel_delimiter=CHANNEL_DELIMITER):
        
        self.data              = data  
        self.path_col          = path_col
        self.count_col         = count_col
        self.channel_delimiter = channel_delimiter   
        self.channel_dict      = {} #dict for incoding channel names
 
    def ChainSplit(self,chain): return chain.split(self.channel_delimiter)
    
    def ChannelDict(self,channel_store):
        '''
        Create `self.channel_dict` indicating each unique channel its unique ID
        Format -> {channel_1 : ID_1,
                     ...
                     ...
                     ...
                   channel_n : ID_n}

        INPUT: [1] `channel_store` list with unique channels
        RETURN: [1] Unit
        '''
        channel_dict = {}

        for ID,channel in enumerate(channel_store):
            channel_dict[channel] = ID
        
        return channel_dict
    
    def UniqueChannel(self):
        '''
        Count number of unique channels in `paths`
        RETURN : [1] Unit
        '''
        
        channel_store = []
        
        size = self.data[self.path_col].shape[0]
        
        for chain in self.data[self.path_col].values:
            channel_store.extend(self.ChainSplit(chain)) #UDF method
        channel_store = set(channel_store)
        
        channel_unique = channel_store #all unique channels in `self.chain_list`
        channel_size = len(channel_store) #size of unique channles
        
        self.channel_dict = self.ChannelDict(channel_unique) #UDF method   
        
        return size,channel_size
        
    
    def ChanneltoID(self,channel_seq): 
        '''
        Convert channel[s] into ID[s]
        INPURT: [1] `channel_seq` list of channels
        RETURN: [1] list of IDs (List[Int])
            '''
        
        return [self.channel_dict[channel] for channel in channel_seq]
        
    def ZeroMatrix(self,row_num ,col_num): 
        '''
        Create zero matrix (size = (number of unique chains) x (number of unique channels))
        '''
        
        return np.zeros((row_num,col_num))
    
    def DecodeDict(self,shapley_dict):
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

        inv_channel_dict = {v: k for k, v in self.channel_dict.items()} # create invert `channel_dict

        for key in shapley_dict.keys():
            decode_dict[inv_channel_dict[key]] = shapley_dict[key]

        return decode_dict
    
    def Vectorization(self):
        '''
        Convert from `chain_list` format to Matrix format.Each ID in matrix is allocated to a channel (see `self.ChannelDict`)
        RETURN: [1] Matrix[Int]. If i-th chain(row) has j-th channel(ID) -> M(i,j) = 1.In case channel(ID) meets multiple
        times in chain , M ignoring it anyway , despite of frequency channel(ID) in chain M(i,j)=1
        '''
        
        size,channel_size = self.UniqueChannel()
        
        M = self.ZeroMatrix(size,channel_size) #create empty Matrix
        for index,chain in enumerate(self.data[self.path_col].values):
            channel_seq = self.ChainSplit(chain)
            target_ID = self.ChanneltoID(channel_seq)
            M[index,target_ID] = 1
            
        return M
            
    def Calc(self,M):
        
        shapley_dict = {}
        
        for i in range(M.shape[1]): # iterate by channels
            mask = M[:,i]>0 # create mask
            M_buffer = M[mask] # rows contiaining i-th channel
            channel_cardinality = M_buffer.sum(axis=1) #channel cardinality           
            conversion = self.data[self.count_col].values[mask] # conversions allocated to i-th channel
            result = np.stack((conversion, channel_cardinality), axis=-1) #
            shapley_dict[i] = np.array(result[:,0]/result[:,1]).sum() # 
            decode_dict = self.DecodeDict(shapley_dict)
        
        return shapley_dict,decode_dict
    
    
    def run(self):
        
        M = self.Vectorization()
        shapley_dict,decode_dict = self.Calc(M)
        
        properties = Properties(self.data,shapley_dict)
        
        if properties.Efficiency():
            print("Properties / Efficience  : +")
        if properties.DummyPlayer(M):
            print("Properties / DummyPlayer : +")
        
        return shapley_dict,decode_dict
    
if __name__ == '__main__':
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--input_filepath', action='store', type=str, required=True)
    my_parser.add_argument('--output_filepath', action='store', type=str, required=True)
    args = my_parser.parse_args()
    
    data = pd.read_csv(args.input_filepath)
    
    if Preprocessing(data).run():
        
        shapley_dict,decode_dict = Shapley(data).run()

        result = pd.DataFrame(decode_dict.items(),columns=[CHANNEL_NAME,SHAPLEY_VALUE])

        result.to_csv(args.output_filepath)
    
    