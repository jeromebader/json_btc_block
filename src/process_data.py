from flask import Flask, request, render_template, redirect, session
from flask import Blueprint
import json
import pandas as pd
import numpy as np
import sys 
import os
import seaborn as sns
import matplotlib.pyplot as plt
import requests
module_path = os.path.dirname(os.path.abspath(__file__))
if module_path not in sys.path:
    sys.path.append(module_path)

import warnings
warnings.filterwarnings('ignore')

## Link to the main APP
process_data= Blueprint('process_data', __name__)
#from src.api_monitor_model import monitor_model


# print(os.getcwd())
# print (os.listdir())
os.chdir(os.path.dirname(os.path.abspath(__file__)))


# Parsing Json to 3 dataframes, 1 for input (vin), 1 for output, 1 for general info for each t

def parsing_btc_block (json_data):
    """
    Function for parsing a btc block to input, output and general transaction data in order
    to compute analysis on the block, regarding the tx values and fees

    Args:
        json_data (json)  : json of the btc chain block

    Returns:
        vin_data (dataframe)  : dataframe containing relevant fields of the transactions\'s input part
        vout_data (dataframe) : dataframe containing relevant fields of the transactions\'s output part
        tx_data (dataframe)   : dataframe containing relevant fields of the transactions\'s info part
    
    """

    # Parsing Json to 3 dataframes, 1 for input (vin), 1 for output, 1 for general info for each tx
    vin_data = pd.DataFrame()
    vout_data = pd.DataFrame()
    tx_data = pd.DataFrame()

    for i,d in enumerate(json_data['result']['tx']):
        print ("---"*5,i,json_data['result']['tx'][i]["txid"])

        try:
            new_tx_row = {
                "txid" : json_data['result']['tx'][i]["txid"], \
                "hash" : json_data['result']['tx'][i]["hash"], \
                "locktime" : json_data['result']['tx'][i]["locktime"], \
                "fee" : json_data['result']['tx'][i]["fee"]
                }
        except:
            new_tx_row = {
                "txid" : json_data['result']['tx'][i]["txid"], \
                "hash" : json_data['result']['tx'][i]["hash"], \
                "locktime" : json_data['result']['tx'][i]["locktime"], \
                "fee" : 0
                }

        # adding row to tx table
        tx_data = tx_data.append(new_tx_row,ignore_index = True)

        for e,c in enumerate(d["vin"]):
            #print (i,"vin: ",e, c)
            try:
            
                # print(p,d["vin"][e]["prevout"][p])
             
                new_vin_row = {\
                "txid":json_data['result']['tx'][i]["txid"],
                "vin_txid":d["vin"][e]["txid"],
                "vin_vout":d["vin"][e]["vout"],
                "vin_prevout_value": d["vin"][e]["prevout"]["value"], 
                "vin_address" : d["vin"][e]["prevout"]["scriptPubKey"]["address"],
                "vin_sequence":d["vin"][e]["sequence"]
                }
                
                # adding row to input table
                vin_data = vin_data.append(new_vin_row,ignore_index = True)
                
                #print(d["vin"][e]["prevout"]["scriptPubKey"]["address"])

            except:
                pass
        

        for ve,vc in enumerate(d["vout"]):
            # print (i,"vout: ", ve, r)
            try:
                print (d["vout"][ve]["scriptPubKey"]["address"])
                new_vout_row = {
                    "txid" :json_data['result']['tx'][i]["txid"], 
                    "vout_n" : d["vout"][ve]["n"], 
                    "vout_value" : d["vout"][ve]["value"], 
                    "vout_address" : d["vout"][ve]["scriptPubKey"]["address"]
                    }
            except:
                print ("no address")
                new_vout_row = {
                    "txid" :json_data['result']['tx'][i]["txid"], 
                    "vout_n" : d["vout"][ve]["n"], 
                    "vout_value" : d["vout"][ve]["value"], 
                    "vout_address" : ''}

            # adding row to the output dataframe
            vout_data = vout_data.append(new_vout_row,ignore_index = True)

    # returning dataframes
    return vin_data,vout_data,tx_data


# seeing the quantity of transactions with output numbers between value1 and value 2

def condition_vout (dfx, value1=1,value2=1):
    """
    function for seeing the quantity of transactions with output numbers between value1 and value 2

    Args:
    dfx (dataframe) : dataframe
    value1 (int) : higher than value
    value2 (int) : lower than value
    

    Returns:
    result (int) : returns counted qty of tx
    """
    
    if value1 >= 101: # batched transactions
        return dfx[ (dfx["vout_n"]>=value1)].count()
    else:
        return dfx[ (dfx["vout_n"]>=value1) & (dfx["vout_n"]<=value2)].count()


### Arguments per JSON
@process_data.route("/process_data", methods=['POST','GET'])
def process():
    """ Function for receiving new DATA by POST of arguments or JSON"""

    filename = session.get('uploaded_file', None)
    print (filename)

    message = ''
    data= {}
   

    with open(filename) as json_file:
    #with open('../uploads/bitcoin_block_750000.json') as json_file:
        json_data = json.load(json_file)


    vin_data,vout_data,tx_data = parsing_btc_block (json_data)

    print ("input txid:",vin_data["txid"].nunique())
    print ("tx txid:",tx_data["txid"].nunique())
    print ("output txid:",vout_data["txid"].nunique())

    # Grouping By txid and counting the output numbers
    dfx = vout_data.groupby("txid")[["vout_n"]].count()
    print (dfx)

    a = condition_vout(dfx, 1,5)
    b = condition_vout(dfx, 6,25)
    c = condition_vout(dfx, 26,100)
    d = condition_vout(dfx, value1=101)

    result_output = {"a. 1 to 5 outputs":a,\
                    "b. 6 to 25 outputs ":b,\
                    "c. 26 to 100 outputs":c,\
                    "d. 101 and + outputs":d \
                    }

    df_result_q1 = pd.DataFrame(result_output).T


    # Visualization of results
    # ax = df_result_q1.sort_values(by="vout_n").plot.barh(figsize=(9, 9))
    # ax.bar_label(ax.containers[0])
    # plt.show()

    # 101 and + outputs (we will call them batched transactions)
    batched_tx = dfx[dfx["vout_n"]>101].reset_index()[["txid"]]

    # getting the fees related with the txid
    merged= pd.merge(tx_data,batched_tx , on="txid", how="inner")

    # print the median fee of batched transactions
    btx_median = merged["fee"].median()
    print ("median fee of batched transactions ", btx_median)

    # show the mean fee of batched transactions
    btx_mean = merged["fee"].mean()
    print ("mean fee of batched transactions ", btx_mean)


    # Getting the consolidated data for the inputs
    input_premerge = vin_data.groupby(by="txid").aggregate({'vin_vout': 'count','vin_prevout_value': 'sum'})

    # Getting the consolidated data for the outputs
    output_premerge = vout_data.groupby(by="txid").aggregate({'vout_n': 'count','vout_value': 'sum'})

    # Merging the grouped input and output tables 
    merge_input_output = pd.merge(input_premerge,output_premerge, how='outer', on=['txid'])

    # resetting index of the grouped df
    merge_input_output = merge_input_output.reset_index()

    merge_input_output = merge_input_output.rename(columns={"vin_vout":"count_inputs",
                                        "vin_prevout_value":"sum_input_values",
                                        "vout_n":"count_outputs",
                                        "vout_value":"sum_output_values"})

    merge_input_output.fillna(0, inplace=True)

    

    df_result_q1_html = df_result_q1.to_html(justify='left',col_space='250px')
    dfdata_html = merge_input_output[merge_input_output['sum_input_values']==merge_input_output['sum_input_values'].max()]
    
   
    dfdata_html = dfdata_html.to_dict()
    #dfdata_html = dfdata_html.to_d
    # pd.DataFrame(dfdata_html).to_html(justify='inherit',col_space="50px")
    print(dfdata_html)
    print(type(dfdata_html))

    return render_template('show_results.html',dfdata_html=dfdata_html,df_result_q1_html = df_result_q1_html,btx_mean=btx_mean,btx_median=btx_median)
    
