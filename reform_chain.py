"""
filepath - папка исходного файлф
filename - имя исходного файла
output_filepath - папка куда сохранять 
output_filename - если название нового файла нужно изменять

запускается функцией run_reform_chain(filepath='', filename='',output_filepath='', output_filename='')
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
    for i in list_chan:
        if i.find('click:')==0:
            index_list.append(list_chan.index(i))
    return index_list

def get_new_chain_list(goal_data=None,sep='=>' ):
    new_chain_list=[]
    new_timeline_list=[]
    for chain,hittime in tqdm(zip(list(goal_data.user_path), list(goal_data.timeline))):
        list_chan=chain.split(sep)
        list_timeline=hittime.split(sep)
    #     print(list_chan)
        #получаем список кликовых каналов 
        index_list=get_index(list_chan)
        #     если в цепи нет кликовых каналов:
        if index_list==[]:
            new_chain_list.append(chain)
            new_timeline_list.append(hittime)
        else:
            for inx in index_list:
                del_inx_l=[]# индексы каналов, которые будут удалены из цепи
                ga_name=list_chan[inx].split(':')[1]# вытаскиваем имя источника га(source/medium/(campaign))

                if inx!=len(list_chan)-1:# если кликовый канал не является последний элементом цепи
                    if list_chan[inx+1]==ga_name:
                        del_inx_l.append(inx+1)
            for di in del_inx_l:#удаляем каналы из цепи
                del list_chan[di]
                del list_timeline[di]
            res_chain=(sep).join(list_chan) # собираем обратно    
            res_timeline=(sep).join(list_timeline)
            new_chain_list.append(res_chain)
            new_timeline_list.append(res_timeline)
    return new_chain_list,new_timeline_list

def safe_result_data(result_path='', name_result_file='',data=None, new_chain=[],new_timeline_list=[], ):
    new_data=data.copy()
    new_data.user_path=new_chain
    new_data.timeline=new_timeline_list
    new_data.to_csv(result_path+name_result_file+'csv',sep=',', index=False, float_format='%.100f')
    return new_data


def run_reform_chain(filepath='', filename='',output_filepath='', output_filename=''):
    """
    filepath - путь к исходному файлу
    filename - имя исходного файла
    output_filepath - куда сохранять 
    output_filename - если название нового файла изменять
    """
    chain_data=open_csv_with_clid(path=filepath+filename)
    new_chain,new_timeline_list=get_new_chain_list(goal_data=chain_data)
    result=safe_result_data(result_path=output_filepath, name_result_file=output_filename,data=chain_data, new_chain=new_chain,new_timeline_list=new_timeline_list)
    return result
    
    
    
    
    