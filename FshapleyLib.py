import pandas as pd
from preprocessing_config import *
from preprocessing_detailed import Preprocessing_detailed
import re
from datetime import datetime
from shapleyLib import shapleyLib
import glob

class FshapleyLib:
    
    def __init__(self,data,sep=SEPARATOR,time_zone=TIME_ZONE,milisec_format=MILISEC_FORMAT):
        self.sep = sep
        self.time_zone = time_zone
        self.milisec_format = milisec_format
        self.data = data

    def Preprocessing(self):

        if self.milisec_format == True:
            divider = THOUSAND
        elif self.milisec_format == False:
            divider = 1
        else:
            raise ValueError("hjhjkhkj")

        secondShift = self.time_zone * MIN_PER_HOUR * SEC_PER_MIN

        self.data['LT_HTS'] = self.data['timeline'].apply(lambda x:int(x.split(TIMELINE_DELIMITER)[-1]))
        
        self.data['LT_SIMPLE_DATE'] = self.data['LT_HTS'].apply(lambda x:datetime.utcfromtimestamp(int(x/divider) + secondShift).strftime('%Y-%m-%d'))
        
        self.data['LT_DATETIME'] = self.data['LT_HTS'].apply(lambda x:datetime.fromtimestamp(int(x/divider) + secondShift))
    
    def intervalCreator(self,date_start,date_finish,freq):

        if re.findall(DAY_PATTERN,freq) != []:
            
            frequency = freq
        
        elif re.findall(MONTH_PATTERN,freq) != []:
            
            frequency = freq
            
        else:
            raise ValueError(
                '''Incorrect frequency format.You can use days and months for exploring periods.
                Example : For days - 1D or 14d, For months - 1m or 2M ''')
            
        
        epochs = pd.date_range(start=date_start, end=date_finish,freq=frequency).to_pydatetime()
            
        epochsInterval = epochCombinator(epochs)

        return epochsInterval
            

    def epochData(self,epoch):
        data_epoch = self.data[(self.data['LT_DATETIME'] >= epoch[0]) &
                               (self.data['LT_DATETIME'] < epoch[1])]
        data_agg = data_epoch.groupby([USER_PATH])[CLIENT_ID].count()
        data_agg = data_agg.to_frame()
        data_agg.reset_index(level=0, inplace=True)
        data_agg.rename(columns={USER_PATH:USER_PATH,CLIENT_ID:COUNT},inplace=True)

        return data_agg
    
    def run(self,date_start,date_finish,freq,output_filename=None,output_filename_order=None):
        
        shp_store      = []
        shpOrder_store = []
        
        Preprocessing_detailed(self.data).run()
        
        self.Preprocessing()

        epochs = self.intervalCreator(date_start,date_finish,freq)
        
        for epoch in epochs:
            data_agg = self.epochData(epoch)
            
            if data_agg.shape[0] != 0:
                shapley_calc,shapley_calcOrder = shapleyLib(data_agg).run()

                shapley_calc[DATE_START] = date_start
                shapley_calc[DATE_FINISH] = date_finish

                shapley_calcOrder[DATE_START] = date_start
                shapley_calcOrder[DATE_FINISH] = date_finish

                shp_store.append(shapley_calc)
                shpOrder_store.append(shapley_calcOrder)
            else:
                shp_store.append(pd.DataFrame())
                shpOrder_store.append(pd.DataFrame())
        
        shapley_calc = pd.concat(shp_store)
        shapley_calcOrder = pd.concat(shpOrder_store)
        
        record_paths = [output_filename,output_filename_order]   
        returnORwrite = all([(val==None) for val in record_paths])
        
        if returnORwrite == True:
            
            return shapley_calc,shapley_calcOrder
        else:
            
            shapley_calc.to_csv(output_filename)
            shapley_calcOrder.to_csv(output_filename_order)
            
    if __name__ == '__main__':
        my_parser = argparse.ArgumentParser()
        my_parser.add_argument('--date_start', action='store', type=str, required=True)
        my_parser.add_argument('--date_finish', action='store', type=str, required=True)
        my_parser.add_argument('--freq', action='store', type=str, required=True)
        my_parser.add_argument('--input_filepath', action='store', type=str, required=True)                                
        my_parser.add_argument('--output_filename', action='store', type=str, required=True)
        my_parser.add_argument('--output_filename_order', action='store', type=str, required=True)
        args = my_parser.parse_args()
        
        frames = [pd.read_csv(file,dtype=object) for file in glob.glob(args.input_filepath)]
        data = pd.concat(frames,axis=0)

        fshapley = FshapleyLib(data)
        shapley_calc,shapley_calcOrder = fshapley.run(args.date_start,
                                                      args.date_finish,
                                                      args.freq,
                                                      args.input_filepath,
                                                      args.output_filename,
                                                      args.output_filename_order)