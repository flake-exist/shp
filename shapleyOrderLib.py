import numpy as np
import pandas as pd
from properties import Properties
from preprocessing_config import *

class shapleyOrderLib:
    
    def __init__(self,data,path_col=USER_PATH,count_col=COUNT,channel_delimiter=CHANNEL_DELIMITER):
        
        self.data = data
        self.path_col          = path_col
        self.count_col         = count_col
        self.channel_delimiter = channel_delimiter  
        
    
    def VectorizationOrder(self,channel_dict):
        '''
        1. Convert chain from String to Array. Each value in Array is allocated to a channel (see `self.ChannelDict`).
        2. Calculate max length of chain in `chain_list`
        
        INPUT: [1] `chain_list`:List[Array(String)] - list with UNIQUE (e.g agrregated data) chains (User paths)
        RETURN: [1] List[Array(Int)], Int
        '''
        max_ChainLength = 0
        list_ChannelEncoded = []

        for chain in self.data[self.path_col].values:
            channel_seq = ChainSplit(chain,self.channel_delimiter)
            channel_seq_id = ElemEncode(channel_seq,channel_dict)
            
            list_ChannelEncoded.append(np.array(channel_seq_id))

            if len(channel_seq_id) > max_ChainLength:
                max_ChainLength = len(channel_seq_id)
            else:
                pass
            
        return list_ChannelEncoded , max_ChainLength
    
    def MatrixOrder(self,list_ChannelEncoded,max_ChainLength):
        '''
        Return: Matrix "ordered". This Matrix created specially for ordered shapley value calculations.
        This Matrix differs from Matrix created in `Shapley` class 'Vectorization' method.
        Each value in Matrix is channelID and each index in columns  is a position of channel in chain
        INPUT: [1] `chain_list`:List[Array(String)] - list with UNIQUE (e.g agrregated data) chains (User paths)
               [2] 'chain_max_length' - max length of chain in `chain_list`
        RETURN:[1] Matrix "ordered" - M_order
        '''
        store = []

        for chain in list_ChannelEncoded:
            empty_path = np.zeros(max_ChainLength)
            empty_path.fill(np.nan)
            empty_path[0:chain.shape[0]] = chain
            '''For concatenate arrays along axis=1 ,add extra dimnension and then transpose to get (1,n) shape,
                where n - number elements in Array'''
            store.append(np.expand_dims(empty_path,axis=1).T)
            
        M_order = np.concatenate(store,axis=0)
            
        return M_order
    
    
    def Cardinality(self,M_order):
        '''Cardinality function'''
        store = []
        for i in range(M_order.shape[0]):
            row = M_order[i,:]
            store.append(np.unique(row[~np.isnan(row)]).shape[0]) #calculate Cardinality
        return np.array(store)
    
    
    def Touchpoint(self,target_M,conversion_target,cardinality_target,frequency_target,positions,ID):
        '''
        Calculate each position value in `examineID` channel
        INPUT:[1] M_order - Ordered Matrix containing channel IDs in the same sequence as they are in string chains
              [2] examineID - channel ID to calculate
              [3] total_conversion - Array with conversions corresponding to chains(sequence IDs) in M_order
        RETURN:Dict
        '''

        pos_dict = {}
        for position in positions:
            pos_mask = (target_M[:,position] == ID)

            conversion_pos = conversion_target[pos_mask]
            cardinality_pos = cardinality_target[pos_mask]
            frequency_pos = frequency_target[pos_mask]

            pos_dict[position] = (conversion_pos/(cardinality_pos*frequency_pos)).sum()            
            pos_dict = FilterTheDict(pos_dict,lambda elem: elem[1] > 0)

        return pos_dict
    
    def FromDictToFrame(self,shapley_DictOrder,columns = [CHANNEL_NAME,POSITION,SHAPLEY_VALUE]):
        store = []
        for key_channel in shapley_DictOrder.keys():
            for key_position in list(shapley_DictOrder[key_channel].keys()):
                row = (key_channel,key_position,shapley_DictOrder[key_channel][key_position])
                store.append(row)
        data = pd.DataFrame(store,columns=columns)
        return data

       
    def Calc(self,M,shapley_DictDecode,encryped_dict,NumAfterPoint=5,error=0.01):
        '''
        Calculate position value in each position in each channel.Return dict ,where keys - channels and its values
        are dicts with position values
        INPUT:[1] M - Matrix got in calculation shapley values (class:Shapley,method:Vectorization)
              [2] shapley_decode - Decoded shapley value dict
              [3] `chain_list`:List[Array(String)] - list with UNIQUE (e.g agrregated data) chains (User paths) 
              [4] total_conversion - total conversions array
        RETURN:Dict[Dict]
        '''
        
        shapley_DictOrder = {}
        
        list_ChannelEncoded, max_ChainLength = self.VectorizationOrder(encryped_dict)
        M_order = self.MatrixOrder(list_ChannelEncoded,max_ChainLength)
        cardinality = self.Cardinality(M_order)
        
        
        for target_channel in shapley_DictDecode.keys():
            ID = encryped_dict[target_channel]
            IDmask = np.where(M[:,ID] == 1)[0] #mask , chains where channel(ID) is in the chain
            
            #masking
            M_target,conversion_target = M_order[IDmask], self.data[self.count_col].values[IDmask]
            cardinality_target,frequency_target = cardinality[IDmask], np.count_nonzero(M_target == ID,axis=1)

            
            '''Make checking'''
            
            position_mask = np.where(M_target==ID)[1]
            positions = np.unique(position_mask)
            
            shapley_DictOrder[target_channel] = self.Touchpoint(M_target,conversion_target,
                                                              cardinality_target,frequency_target,
                                                              positions,ID)
            
            touchpointValSum = np.round(sum(shapley_DictOrder[target_channel].values()),NumAfterPoint)
            shapleyVal = np.round(shapley_DictDecode[target_channel],NumAfterPoint)
            
            if abs(touchpointValSum - shapleyVal) < shapleyVal * error:
                pass
            else:
                raise ValueError("Required : Sum of {0} touchpoint values - {1} | Found : Sum of {0} touchpoint values - {2}".
                                format(target_channel,touchpointValSum,shapleyVal))
                
            df = self.FromDictToFrame(shapley_DictOrder)
                
                
        return df
   