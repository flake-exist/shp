"""
установить библиотеку PuLP если не установлена: !pip install PuLP

запускать функцией run_budget(budget_path='', atrubution_res_path='',outputpath='', project_id=3, mode='profile')
"""


import os
import pandas as pd
import numpy as np
from functools import reduce
import glob
import argparse
from pulp import *

class CalcBudget:
    def __init__(self,template,budget_path, atrubution_res_path,outputpath,project_id,mode):
        self.budget_path=budget_path
        self.atrubution_res_path=atrubution_res_path
        self.outputpath=outputpath
        self.project_id=project_id        
        #TODO: 
        self.template = template
        self.mode=mode # "creative"/"profile". default-'profile'
    
    def get_full_shapley_result(self):
        
        path_list = glob.glob(self.atrubution_res_path + self.template)

        df_list=[]
        for path in path_list:
            goal=self.get_goal(path)
            data=pd.read_csv(path, sep=',')
            data['NN']=goal
            data['variable']='shapley'
            df_list.append(data)

        shapley_result = reduce(lambda up,down: pd.concat([up, down],
                       axis=0, sort=False,ignore_index=False), df_list) 

        shapley_result.reset_index(inplace=True)
        shapley_result.drop(['index','Unnamed: 0'], axis=1, inplace=True)
        return shapley_result

    def get_goal(self,path):
        return int(path.split('_')[1])
    
    def read_budget_data(self):
        #budget_data = pd.read_excel(self.budget_path)
        budget_data = pd.read_csv(self.budget_path,sep=',', encoding='utf-8')
        budget_data.profile=budget_data.profile.astype(str)
        if self.mode=='creative':
            budget_data.term=budget_data.term.astype(str)
        return budget_data    
    
    def get_profile_from_channel_name(self, attr_result=None):
        list_need_channel=[]
        for i in attr_result.channel_name:
            if len(i.split(':'))>1:
                list_need_channel.append(i.split(':')[-1])
            else:
                list_need_channel.append('-')
        return list_need_channel
    
    def get_UTM_TERM_from_channel_name(self, attr_result=None):
        list_need_channel=[]
        for i in attr_result.channel_name:
            term = i.split(' / ')[-1]
            if len(term) != 0:
                list_need_channel.append(term.split(':')[0])
            else:
                list_need_channel.append('-')
        return list_need_channel
    
    def _get_max_use_conv(self,data=None,number_of_goal=[]):
        conv_dict={}
        for goal in number_of_goal:
            df = data[data.NN==goal]
            mark = df[df.variable=='shapley'].shapley_value.sum()
            conv_dict[goal] = mark
        if 0 not in conv_dict.keys():
            conv_dict[0]=0
        return conv_dict
    
    
    def Debil(self,input_dict,total):

        if sum(input_dict.values())==0:
            return input_dict

        else:

            goal_dict_sorted = {k: v for k, v in sorted(input_dict.items(), key=lambda item: item[1])}

            prob=LpProblem("CPA problem",LpMinimize)

            decision_vars_dict = {"x_{0}".format(i) : LpVariable("{}".format(i),0,None,LpContinuous ) for i in goal_dict_sorted.keys()}

            x_list = [decision_vars_dict[i] for i in decision_vars_dict.keys()]
            c_list = [i for i in goal_dict_sorted.values()]

#             print(x_list)
#             print(c_list)
            prob += sum([l * (1/r) for l,r in zip(x_list,c_list)]) - sum(x_list) * 1/(sum(c_list)) , "Budget Distribution"

            #constraints
            prob += sum(x_list) == total,"const_budget"
            for i in range(len(x_list) - 1):
                prob += x_list[i]*(1/c_list[i]) >= x_list[i+1]*(1/c_list[i+1]),"constraint{0}".format(i)

            # prob.writeLP("BudgetDistrib.lp")
            if prob.solve() == 1:     
                # print("Status:", LpStatus[prob.status])
                r = {v.name : v.varValue for v in prob.variables()}
                return r

            else:

                r = {v : 0 for v in goal_dict_sorted.keys()}
                return r    
    
    

    def make_new_format(self,data=None):
        
        data.rename(columns={'clicks':'click_api','impressions':'impressions_api'}, inplace=True)
        if self.mode=='creative':
           
            res_2=data[['profile', 'source', 'medium', 'campaign', 'term' ,'click_api','impressions_api','budget_fact',
                        'shapley_value',
                   'date_start', 'date_finish', 'NN', 'variable', 'type','total_sum', 'CPM']]

            res_3=pd.pivot_table(res_2, values='shapley_value', index=['profile','source', 'medium', 'campaign', 'term',
                                                                       'impressions_api','NN','click_api', 'budget_fact',
                                                                       'variable',
                                                                'date_start', 'date_finish','total_sum'],
                          columns=['type'], aggfunc=np.sum)     
            
            
        elif self.mode=='profile':
            
            res_2=data[['profile', 'source', 'medium', 'campaign','click_api','impressions_api','budget_fact',
                        'shapley_value',
                   'date_start', 'date_finish', 'NN', 'variable', 'type','total_sum', 'CPM']]
            res_3=pd.pivot_table(res_2, values='shapley_value', index=['profile','source', 'medium', 'campaign',
                                                                       'impressions_api','NN','click_api', 'budget_fact',
                                                                       'variable',
                                                                'date_start', 'date_finish','total_sum'],
                          columns=['type'], aggfunc=np.sum)

        res_3.reset_index(inplace=True)

        res_3.fillna(0, inplace=True)

        res_3['total_shapley_value']=res_3.click+res_3.view
        
        res_3['channel_number']=self.get_channel_number(data=res_3)
        return res_3


    def get_channel_number(self,data=None):
        #make help df

        if self.mode=='creative':
            dft=pd.DataFrame()
            dft['channel_tuple']=list(set(list(zip(data.source, data.medium, data.campaign,data.term))))
            dft['channel_number']=range(len(dft.channel_tuple))
            num_list=[dft[dft.channel_tuple==i].channel_number.values[0] 
                      for i in zip(data.source, data.medium, data.campaign,data.term)]
            return num_list
        
        elif self.mode=='profile':
            dft=pd.DataFrame()
            dft['channel_tuple']=list(set(list(zip(data.source, data.medium, data.campaign))))
            dft['channel_number']=range(len(dft.channel_tuple))
            num_list=[dft[dft.channel_tuple==i].channel_number.values[0] 
                      for i in zip(data.source, data.medium, data.campaign)]
            return num_list

        return num_list


    def make_dict(self, data=None):
        big_dict={}
        for num in data.channel_number:
            big_dict[num]=({i:j for i,j in zip(data[data.channel_number==num].NN,
                                              data[data.channel_number==num].total_shapley_value) if j!=0},
                           data[data.channel_number==num].budget_fact.values[0] )
        return big_dict


    def run_DEBIL(self,input_dict=None):
        
        out_big_dict={}
        for k,v in input_dict.items():
            out_big_dict[k]=self.Debil(v[0],v[1])
        return out_big_dict


    def calc_cpm(self, data):
        ll=[]
        for b,i in zip(data.budget_fact,data.impressions):
            if i!=0:
                ll.append((b/i)/1000)
            else:
                ll.append(0)
        return ll



    def calc_metrics(self, attr_result=None,budget_data=None ):
        attr_result['profile']=self.get_profile_from_channel_name(attr_result=attr_result)
        
        if self.mode=='creative':
            attr_result['term']=self.get_UTM_TERM_from_channel_name(attr_result=attr_result)
            result_full_money=budget_data.merge(attr_result, on=['profile','term'], how='left')
        else:
            result_full_money=budget_data.merge(attr_result, on='profile', how='left')

        result_full_money.fillna(0, inplace=True)
        
        conv_dict=self._get_max_use_conv(data=attr_result,number_of_goal=attr_result.NN.unique())
        
        result_full_money['type']=result_full_money.channel_name.apply(lambda row: row.split(':')[0] if row!=0 else 0)
        result_full_money['CPM'] = self.calc_cpm(result_full_money)
        result_full_money['total_sum']=result_full_money.NN.apply(lambda row: conv_dict[row])
#         return result_full_money
        result_full_money=self.make_new_format(data=result_full_money)
        
        big_dict=self.make_dict(data=result_full_money)

        dict_from_debil=self.run_DEBIL(input_dict=big_dict)
        
        result_full_money['new_goal_budget']=0
        
        for k,v in dict_from_debil.items():
            for kk,vv in v.items():
                indx=result_full_money[(result_full_money.channel_number==k) & (result_full_money.NN==int(float(kk)))].index[0]
                result_full_money.new_goal_budget.iloc[indx]=vv

        result_full_money['new_CPA']=result_full_money.new_goal_budget/result_full_money.total_shapley_value 
        
        result_full_money['normal_CPA']=result_full_money.budget_fact/result_full_money.total_shapley_value
        result_full_money.new_CPA=np.round(result_full_money.new_CPA.values,1)
        result_full_money.normal_CPA=np.round(result_full_money.normal_CPA.values,1)
        return result_full_money  
    
    def check_result(self, data):
        for num in data.channel_number.unique():
#             print(data[data.channel_number==num].NN.unique())
            budget_fact=data[data.channel_number==num].budget_fact.unique()[0]
            goal_budget=list(data[data.channel_number==num].new_goal_budget)
            if round(budget_fact,1)==round(sum(goal_budget),1) or data[data.channel_number==num].NN.unique()[0]==0:
                continue
            else:
                print('channel num:',num,'|','FALSE','|','budget_fact:',budget_fact,'|','goal_budget:',sum(goal_budget))
        return 
      

    def safe_file(self, data_to_safe):
        data_to_safe.to_csv(self.outputpath, index=False)
        return data_to_safe
        
def run_budget(template= '', budget_path='', atrubution_res_path='',outputpath='',project_id=3,
               mode='profile'):
    
    cb=CalcBudget(template,budget_path, atrubution_res_path,outputpath,project_id,depth_attr,mode) 
    
    shapley_result = cb.get_full_shapley_result()
    
    budget_data=cb.read_budget_data()
    result_full_money= cb.calc_metrics(attr_result=shapley_result,budget_data=budget_data )
    result_full_money=cb.check_result(result_full_money)
    result_full_money=cb.safe_file(result_full_money) 
    return result_full_money



if __name__ == '__main__':
    
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument('--budget_path', action='store', type=str, required=True)                                
    my_parser.add_argument('--atrubution_res_path', action='store', type=str, required=True)
    my_parser.add_argument('--outputpath', action='store', type=str, required=True)
    args = my_parser.parse_args()
    
    res = run_budget(args.budget_path, args.atrubution_res_path, args.outputpath)
