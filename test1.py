#Connecting python with big query
from datetime import date,timedelta
import datetime
import time
from dateutil import parser
from google.cloud import bigquery
import pandas 
client = bigquery.Client()
dimensions=["App_URL_ID","City_ID"]
x=len(dimensions)
string_dimensions=' '.join(dimensions)
string_dimensions=string_dimensions.replace(" ",",")
#taking input metrics from user
inputs=[]
n = int(input("Enter number of input metrics: ")) 
for i in range(0,n):
    ele = input()
    inputs.append(ele)
outputs=[]
m = int(input("Enter number of output metrics: ")) 
for j in range(0,m):
    temp = input()
    outputs.append(temp)
#days_analysis=int(input("Enter no of days for performing analysis:"))

def prepend(list, str):
    # Using format()
    str += '{0}'
    list = [str.format(i) for i in list]
    return(list)
str="sum("
inputs_v2=prepend(inputs,str)
inputs_v2 = [item + ')' for item in inputs_v2]
#string_inputs=' '.join(inputs_v2)
#string_inputs=string_inputs.replace(" ",",")
string3=""
string4=""
for l in range(0,n):
    string3+=inputs_v2[l]+" "+"as"+" "+inputs[l]+","
outputs_v2=prepend(outputs,str)
outputs_v2 = [item + ')' for item in outputs_v2]
for l in range(0,m):
    string4+=outputs_v2[l]+" "+"as"+" "+outputs[l]+","
#string_outputs=' '.join(outputs_v2)
#string_out=string_inputs.replace(" ",",")
string_final=string3+string4
#print(string_final)
#current_date = date.today().isoformat()   
#days_before =(date.today()-timedelta(days=days_analysis))
#days_before = parser.parse(days_before)
#print(days_before)
#print(type(days_before))
for i in range(0,x):
    sql="SELECT"+" "+string_final+dimensions[i]+" "+"FROM `deeplake.dv3_attribution_raw.cashify` where Date>'2022-07-23' group by"+" "+dimensions[i]
    results= client.query(sql)
    df=results.to_dataframe()
    df2=results.to_dataframe()
    df.fillna(0,inplace=True)
    df2.fillna(0,inplace=True)
    df['max_percentage_of_input']=0
    df['max_percentage_of_output']=0
    for j in range(0,n):
        #globals()[f'Total_no_of_{inputs[j]}'] = df[inputs[j]].sum()
        Total_no_of_inputs=df[inputs[j]].sum()
        Total_no_of_outputs=df[outputs[j]].sum()
        df[f'cummulative_sum_{inputs[j]}']=df[inputs[j]].cumsum()      
        df[f'cummulative_percentage_{inputs[j]}']=df[f'cummulative_sum_{inputs[j]}'].div(Total_no_of_inputs)
        df[f'cummulative_percentage_{inputs[j]}']=df[f'cummulative_percentage_{inputs[j]}']*100
        df['max_percentage_of_input']=df[["max_percentage_of_input",f"cummulative_percentage_{inputs[j]}"]].max(axis=1)
    for k in range(0,m):
        globals()[f"Total_no_of_{outputs[k]}"] = df[outputs[k]].sum()
        df[f'cummulative_sum_{outputs[k]}']=df[outputs[k]].cumsum()  
        #df[f'cummulative_percentage_{outputs[k]}']=df[f'cummulative_sum_{outputs[j]}'].div(f'Total_no_of_{outputs[j]}'.values,axis=0)
        df[f'cummulative_percentage_{outputs[k]}']=df[f'cummulative_sum_{outputs[j]}'].div(Total_no_of_outputs)
        df[f'cummulative_percentage_{outputs[k]}']=df[f'cummulative_percentage_{outputs[k]}']*100
        df['max_percentage_of_output']=df[["max_percentage_of_output",f"cummulative_percentage_{outputs[k]}"]].max(axis=1)
    df['max_percentage_of_input_output']=df[["max_percentage_of_input","max_percentage_of_output"]].max(axis=1)
    if(df.at[0,'max_percentage_of_input_output']<=5.000000):    
        long_tail_sum=[]
        for l in range(0,n):
            long_tail_sum.append(df.query("max_percentage_of_input_output<=5")[inputs[l]].sum())
        for h in range(0,m):
            long_tail_sum.append(df.query("max_percentage_of_input_output<=5")[outputs[h]].sum())
        long_tail_sum.append("<NA>")
        row_number=df[df['max_percentage_of_input_output']>5.000000].index[0]
        #print(row_number)
        df2.drop(df2.index[:row_number], inplace=True)    
        
        first_valid_index=df.first_valid_index()
        df2.loc[first_valid_index] =long_tail_sum
    for i in range(0,n):
        for j in range(0,m):
            Avg=df2[inputs[i]].sum()/df2[outputs[j]].sum()
            df2[f"performance_Index_{inputs[i]}_{outputs[j]}"]=Avg/((df2[f"{inputs[i]}"]+Avg)/(df2[f"{outputs[j]}"]+1))
            Standard_deviation=df2[f"performance_Index_{inputs[i]}_{outputs[j]}"].std()
            #print("Standard deviation for"+" "+dimensions[i]+" "+"for metrics "+inputs[i]+"_"+outputs[j]+" "+"is",Standard_deviation)
            print("Standard Deviation:",Standard_deviation)
    #print(df)
    epoch = time.time()
    Logical_IO_Id = "%d" %(epoch)
    print(Logical_IO_Id)
    print(df2)
