"""
filepath - папка исходного файлф
filename - имя исходного файла
output_filepath - папка куда сохранять 
output_filename - если название нового файла нужно изменять

type_data=agg/detailed

запускается функцией run_reform_chain(filepath='',
                     filename='',
                     output_filepath='',
                     output_filename='',
                     test_output_filename='',
                     type_data='agg',
                     chain_sep='=>',
                     channel_sep='_>>_',
                     len_ga_channel=5)
                      
#TODO 
добавлен новый файл для проверки:
'user_path' - оригинальные цепи,
'count' - количество цепей,
'new_path' - новые цепи после выполнения скрипта,
'len_orig' - старая длина цепи,
'len_new_chain' - новая длина цепи,
'index_to_dalite' - индексы, которые были удалены,
'check' - для удобства фильтрации: 1-изменен,0-безизменений

new: 
len_ga_channel - количество частей из которых состоит ГАшный канал.

"""



import csv
import itertools
import pandas as pd
import numpy as np
from functools import reduce
from tqdm import tqdm_notebook as tqdm

def open_csv_with_clid(path=''):
    with open(path) as f:
        reader = csv.reader(f)
        listrow=[]
        for row in reader:
            listrow.append(row)
    return pd.DataFrame(listrow[1:], columns=listrow[0])

## поиск канала в цели который начинаеттся с "click:"
def get_index(list_chan):
    """
    list_chan - цепь рассплитованная по сепаратору
    return - список индексов "кликовых" каналов
    """
    index_list=[]
    for ind,chan in enumerate(list_chan):
        if chan.find('click_')==0:
            index_list.append(ind)
    return index_list

def get_new_chain_list(goal_data=None,chain_sep='=>',channel_sep='_>>_',len_ga_channel=5 ):
    new_chain_list=[]
    new_timeline_list=[]
    del_inx_list_upper=[]
    for chain,hittime in tqdm(zip(list(goal_data.user_path), list(goal_data.timeline))):
        list_chan=chain.split(chain_sep)
        list_timeline=hittime.split(chain_sep)
    #     print(list_chan)
        #получаем список кликовых каналов 
        index_list=get_index(list_chan)
        #     если в цепи нет кликовых каналов:
        if index_list==[]:
            new_chain_list.append(chain)
            new_timeline_list.append(hittime)
            del_inx_list_upper.append([])
        else:
            del_inx_l=[]# индексы каналов, которые будут удалены из цепи
            for inx in index_list:                
                ga_name=(channel_sep).join(list_chan[inx].split(channel_sep)[1:len_ga_channel+1])# вытаскиваем имя источника га(source/medium/(campaign))
                if inx!=len(list_chan)-1:# если кликовый канал не является последний элементом цепи
                    if list_chan[inx+1]==ga_name:
                        del_inx_l.append(inx+1)
            del_inx_list_upper.append([del_inx_l])
            for di in reversed(del_inx_l):#удаляем каналы из цепи
                del list_chan[di]
                del list_timeline[di]
            res_chain=(chain_sep).join(list_chan) # собираем обратно    
            res_timeline=(chain_sep).join(list_timeline)
            new_chain_list.append(res_chain)
            new_timeline_list.append(res_timeline)
    return new_chain_list,new_timeline_list,del_inx_list_upper


def get_new_chain_list_for_agg(goal_data=None,chain_sep='=>',channel_sep='_>>_',len_ga_channel=5 ):
    new_chain_list=[]
    del_inx_list_upper=[]
    for chain in tqdm(list(goal_data.user_path)):
        list_chan=chain.split(chain_sep)
        #получаем список кликовых каналов 
        index_list=get_index(list_chan)
        #     если в цепи нет кликовых каналов:
        if index_list==[]:
            new_chain_list.append(chain)
            del_inx_list_upper.append([])
        else:
            del_inx_l=[]# индексы каналов, которые будут удалены из цепи
            for inx in index_list:                
                ga_name=(channel_sep).join(list_chan[inx].split(channel_sep)[1:len_ga_channel+1])# вытаскиваем имя источника га(source/medium/(campaign))
                if inx!=len(list_chan)-1:# если кликовый канал не является последний элементом цепи
                    if list_chan[inx+1]==ga_name:
                        del_inx_l.append(inx+1)
            del_inx_list_upper.append(del_inx_l)
            for di in reversed(del_inx_l):#удаляем каналы из цепи
                del list_chan[di]
            res_chain=(chain_sep).join(list_chan) # собираем обратно    
            new_chain_list.append(res_chain)
    return new_chain_list,del_inx_list_upper

def safe_result_data(result_path='', name_result_file='',test_output_filename='',
                     data=None, new_chain=[],new_timeline_list=[],
                     del_inx=[],type_data='agg',chain_sep='=>' ):
    new_data=data.copy()
    data['new_path']=new_chain
    data['len_orig']=data.user_path.apply(lambda row:len(row.split(chain_sep)))
    data['len_new_chain']=data.new_path.apply(lambda row:len(row.split(chain_sep)))
    data['index_to_dalite']=del_inx
    data['check']=data.len_orig!=data.len_new_chain
    data.check=data.check.astype(int)
    new_data.user_path=new_chain
    if type_data=='detailed':
        new_data.timeline=new_timeline_list
        
    elif type_data=='agg':
        data['new_path']=new_chain        
        new_data=regroup_data(new_data)
    data.to_csv(result_path+test_output_filename, sep=',', index=False,float_format='%.100f')    
    new_data.to_csv(result_path+name_result_file+'csv',sep=',', index=False, float_format='%.100f')
    return data

def regroup_data(res):
    a=res.groupby('user_path').sum()
#     a['count']=a['count'].astype(int)
#     print(a['count'].sum())
    a.reset_index(inplace=True)
    return a


def run_reform_chain(filepath='',
                     filename='',
                     output_filepath='',
                     output_filename='',
                     test_output_filename='',
                     type_data='agg',
                     chain_sep='=>',
                     channel_sep='_>>_',
                     len_ga_channel=5):
    """
    filepath - путь к исходному файлу
    filename - имя исходного файла
    output_filepath - куда сохранять 
    output_filename - если название нового файла изменять
    """
    chain_data=open_csv_with_clid(path=filepath+filename)
    chain_data['count']=chain_data['count'].astype(int)
#     print(chain_data['count'].sum())
    if type_data=='agg':
        new_chain,del_inx_list_upper=get_new_chain_list_for_agg(goal_data=chain_data,
                                                                chain_sep='=>',
                                                                channel_sep='_>>_',
                                                                len_ga_channel=5 )
        result=safe_result_data(result_path=output_filepath,
                                name_result_file=output_filename,
                                test_output_filename=test_output_filename,
                                data=chain_data,
                                new_chain=new_chain,
                                del_inx=del_inx_list_upper,
                                type_data='agg',
                               chain_sep='=>')
    elif type_data=='detailed':
        new_chain,new_timeline_list,del_inx_list_upper=get_new_chain_list(goal_data=chain_data,
                                                                          chain_sep='=>',
                                                                          channel_sep='_>>_',
                                                                          len_ga_channel=5)
        result=safe_result_data(result_path=output_filepath, name_result_file=output_filename,
                                test_output_filename=test_output_filename,
                                data=chain_data, new_chain=new_chain,new_timeline_list=new_timeline_list,
                                del_inx=del_inx_list_upper,
                                type_data='detailed',chain_sep='=>')
    return result
    
    
