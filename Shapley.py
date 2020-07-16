import pandas as pd
import numpy as np
import argparse
from Verification import Verification
from Properties import Properties
from config import *

class Shapley:
    
    def __init__(self,data):
        
        self.data            = data
        self.channel_id_dict = None
        
    def PathStats(self):
        
        path_count = self.data[USER_PATH].shape[0] #path_count

        self.data[CHANNEL_SEQ] = self.data[USER_PATH].apply(lambda x:ChainSplit(x,CHANNEL_DELIMITER))
        channel_list             = self.data[CHANNEL_SEQ].values

        flat_channel_list = [channel for sublist in channel_list for channel in sublist]
        path_length_max   = max([len(sublist) for sublist in channel_list]) #path_length_max

        unique_channel_list  = set(flat_channel_list)
        self.channel_id_dict = GetEncoding(unique_channel_list)
        unique_channel_count = len(unique_channel_list) #unique_channel_count
        
        return path_count,unique_channel_count,path_length_max
    
    def Vectorization(self,path_count,unique_channel_count,path_length_max):
        
        M = np.empty((path_count,path_length_max))
        M.fill(np.nan)
        
        for index,path in enumerate(self.data[CHANNEL_SEQ]):
            path_encoded = SequenceEncode(path,self.channel_id_dict)
            M[index,0:path_encoded.shape[0]] = path_encoded
        
        return M
    
    def Classic(self,M):
        
        shapley_Encoded = {}
        
        for id_ in self.channel_id_dict.values():
            mask = np.unique(np.where(M == id_)[0]) #unique rows containing id_
            M_buffer = M[mask,:]
            cardinality = Cardinality(M_buffer)
            conversion = self.data[COUNT].values[mask] # conversions allocated to i-th channel
            M_calc = np.stack((conversion, cardinality), axis=-1) #
            shapley_Encoded[id_] = np.array(M_calc[:,0]/M_calc[:,1]).sum()
            
        shapley_classic = DecodeDict(shapley_Encoded,self.channel_id_dict)
        
        Properties(self.data,shapley_classic).run() #Check Properties
         
        return shapley_classic
    
    def Touchpoint(self,M_buffer,conversion_buffer,cardinality_buffer,frequency_buffer,positions,id_):
        
        '''
        Calculate each position value in `examineID` channel
        INPUT:[1] M_order - Ordered Matrix containing channel IDs in the same sequence as they are in string chains
              [2] examineID - channel ID to calculate
              [3] total_conversion - Array with conversions corresponding to chains(sequence IDs) in M_order
        RETURN:Dict
        '''

        pos_dict = {}
        for position in positions:
            mask = (M_buffer[:,position] == id_)
            conversion_pos = conversion_buffer[mask]
            cardinality_pos = cardinality_buffer[mask]
            frequency_pos = frequency_buffer[mask]

            pos_dict[position] = (conversion_pos / (cardinality_pos * frequency_pos)).sum()            


        return pos_dict
    
    def Order(self,M,shapley_classic=None):
        
        shapley_EncodedOrder = {}
        
        for id_ in self.channel_id_dict.values():
            mask = np.unique(np.where(M == id_)[0]) #unique rows containing id_
            M_buffer = M[mask,:]
            conversion_buffer = self.data[COUNT].values[mask]
            cardinality_buffer = Cardinality(M_buffer)
            frequency_buffer = np.count_nonzero(M_buffer == id_,axis=1)
            positions = np.unique(np.where(M_buffer==id_)[1])
            
            shapley_EncodedOrder[id_] = self.Touchpoint(M_buffer,
                                                                   conversion_buffer,
                                                                   cardinality_buffer,
                                                                   frequency_buffer,
                                                                   positions,
                                                                   id_)
            
        shapley_order = DecodeDict(shapley_EncodedOrder,self.channel_id_dict)
        
        if shapley_classic == None:
            
            return shapley_order
        else:
            
            for channel in shapley_classic.keys():
                
                channel_touchpoint_val_sum = np.round(sum(shapley_order[channel].values()),PRECISION)
                channel_val = np.round(shapley_classic[channel],PRECISION)
                
                if abs(channel_touchpoint_val_sum - channel_val) < channel_val * ERROR:
                    pass
                else:
                    print(SHAPLEY_ORDER_ERROR.format(channel,channel_touchpoint_val_sum,channel_val))
                    
            return shapley_order
            
    
    def run(self,date_start=None,date_finish=None):
        
        Verification(self.data).run() # Verification
        
        path_count,unique_channel_count,path_length_max = self.PathStats()
        
        M = self.Vectorization(path_count,unique_channel_count,path_length_max)
        
        shapley_classic = self.Classic(M)
        
        shapley_order   = self.Order(M,shapley_classic)
        
        shapley_classic_df = pd.DataFrame(shapley_classic.items(),columns=[CHANNEL_NAME,SHAPLEY_VALUE])
        shapley_classic_df[DATE_START],shapley_classic_df[DATE_FINISH]  = [date_start,date_finish]
        
        shapley_order_df   = ShapleyOrderToFrame(shapley_order)
        shapley_order_df[DATE_START],shapley_order_df[DATE_FINISH]= [date_start,date_finish]
        
        return shapley_classic_df,shapley_order_df
    
if __name__ == '__main__':
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--input_filepath', action='store', type=str, required=True)                                
    my_parser.add_argument('--output_filepath', action='store', type=str, required=True)
    my_parser.add_argument('--output_filepathe_order', action='store', type=str, required=True)
    args = my_parser.parse_args()
    
    data = pd.read_csv(args.input_filepath)
    
    shapley = Shapley(data)
    shapley_classic_df,shapley_order_df = shapley.run() 
    
    shapley_classic_df.to_csv(args.output_filepath)
    shapley_order_df.to_csv(args.output_filepathe_order)