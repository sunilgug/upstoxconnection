#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 00:55:21 2019

@author: sunilguglani
"""
import multiprocessing
import pandas as pd
import time
import requests
import sys
from upstox_api.utils import LiveFeedType
global df_all
global q1
global t
t=5

def assign_values(x):
    
    global q1
    '''
    q1,q2=x[0],x[1]
    '''
    q1=x[0]

lst_columns=['symbol','timestamp','bids_price_1','asks_price_1','bids_quantity_1','asks_quantity_1'
             ,'bids_price_0','asks_price_0','bids_quantity_0','asks_quantity_0','live_ltp']
df_all=pd.DataFrame(columns=lst_columns)
df_all['bids_price_1']=0.00
df_all['asks_price_1']=0.00
def event_handler_quote_update(message):
    #print(message)
    global df_all
    global q1

    json=message
    a=time.time()
    try:
        if (len(json)>0):
            #if (len(df_all[df_all.symbol==json['symbol'].upper()])>0):
            #print("json>0")
            lst_values=[]
            if (json['exchange']!='NSE_INDEX'):
                if  ((int(json['bids'][1]['price'])>0)&
                    (int(json['asks'][1]['price'])>0) & 
                    (int(json['bids'][1]['quantity'])>0)&
                    (int(json['asks'][1]['quantity'])>0)):
    
                        #print('inside ',json['symbol'].upper())
                        lst_values=[json['symbol'].upper(),int(json['timestamp']), float(json['bids'][1]['price']),float(json['asks'][1]['price']),
                                 int(json['bids'][1]['quantity']), int(json['asks'][1]['quantity']),
                                  float(json['bids'][0]['price']),float(json['asks'][0]['price']),
                                 int(json['bids'][0]['quantity']), int(json['asks'][0]['quantity']),
                                 0]
                                 #float(json['live_ltp'])]
                                 
                                 
            elif (json['exchange']=='NSE_INDEX'):
                    lst_values=[json['symbol'].upper(),int(json['timestamp']), 0,0,0,0,0,0,0,0,
                             float(json['live_ltp'])
                             ]
                    
            df_temp=pd.DataFrame([lst_values],columns=lst_columns)
            if (len(df_all)>0):                        
                df_all=df_all[df_all.symbol!=json['symbol'].upper()].append(df_temp)
            else:
                df_all=df_temp.copy()
    #print('here it is')

        #print(df_all)
        q1.put(df_all)
        #q2.put(json['symbol'].upper())
    except (RuntimeError, TypeError, NameError,requests.HTTPError) as e:
        print ("block1 function: ",e)

def websocket(u):    
    u.set_on_quote_update(event_handler_quote_update)
    # Start the websocket
    u.start_websocket(True)


def subscribe(u, lst):
    
    for l in lst:#lst_q_sets:
        print('lst ',l[0],' ',l[1])
        lst_symb_exc=l[0]
        for sym in lst_symb_exc:
            
            print('sym ',sym)
            inst=u.get_instrument_by_symbol(sym[1],sym[0])
            print(inst)

            u.subscribe(inst, LiveFeedType.Full)
            
def unsubscribe_instruments_single(self,inst):
    u.unsubscribe(inst, LiveFeedType.Full)

def unsubscribe_instruments(u, lst):
    
        for sym in lst:
            
            print('sym ',sym)
            inst=u.get_instrument_by_symbol(sym[1],sym[0])
            print(inst)

            u.unsubscribe(inst, LiveFeedType.Full)
