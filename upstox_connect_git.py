#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb  9 22:50:07 2019

@author: sunilguglani
"""
from upstox_api.utils import LiveFeedType

from upstox_api.api import *
from pandas.io.json import json_normalize

import pandas as pd
import numpy as np
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ChromeOptions, Chrome
from selenium import webdriver
import time
import sys
import requests
import os

class Connect_broker:
    def __init__(self, username):
        login_url='https://docs.google.com/spreadsheets/d'
        resp=requests.get(login_url)
        
        df_cred=pd.read_html(resp.content)[0]
        df_cred.columns=df_cred[:].iloc[0]
        df_cred.drop(0,axis=0,inplace=True)
        df_cred.drop(df_cred.columns[0],axis=1,inplace=True)
        
        df_cred=df_cred[df_cred.User==str(username).upper()].copy()
        df_cred.reset_index(drop=True,inplace=True)
        self.User=df_cred['User'].iloc[0]
        self.chromedriver_loc=df_cred['chromedriver_loc'].iloc[0]
        self.login_id=df_cred['login_id'].iloc[0]
        self.password=df_cred['password'].iloc[0]
        self.password2=df_cred['password2'].iloc[0]
        self.your_api_key=df_cred['your_api_key'].iloc[0]
        self.your_redirect_uri=df_cred['your_redirect_uri'].iloc[0]
        print(self.your_redirect_uri)
        self.your_api_secret=df_cred['your_api_secret'].iloc[0]
        

    def Connect(self):
        
        ''' **********************'''
        __file__=self.chromedriver_loc
        PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        DRIVER_BIN = os.path.join(PROJECT_ROOT, self.chromedriver_loc)
        
        driver = webdriver.Chrome(executable_path = DRIVER_BIN)
        
        login_url=""
        login_id=self.login_id
        password=self.password
        password2=self.password2
        ''' **********************'''
    
        your_api_key=self.your_api_key

        your_redirect_uri=self.your_redirect_uri
        your_api_secret=self.your_api_secret
        
        s = Session (your_api_key)
        s.set_redirect_uri (your_redirect_uri)
        s.set_api_secret (your_api_secret)
        #Get the login URL so you can login with your Upstox UCC ID and password.
        login_url=s.get_login_url()
        print (login_url)
        
        ''' **********************'''
    
        driver.get(login_url)
        driver.find_element_by_id("name").send_keys(login_id)
        driver.find_element_by_id("password").send_keys(password)
        driver.find_element_by_id("password2fa").send_keys(password2)
        driver.find_element_by_id("close-login").submit()
        time.sleep(15)
        driver.find_element_by_id("allow").submit()
        
        
        url=driver.current_url
    
        url=str(url)
        url_substr='http://127.0.0.0/?code='
        while True:
            if url_substr in (url):
                print(url)
                code=url.split("=")
                access_code=code[1]
                break
            else:
                time.sleep(5)
                
        print(access_code)
    
        ''' **********************'''
        driver.quit()
    
        
        ## this will return a URL such as https://api.upstox.com/index/dialog/authorize?apiKey={your_api_key}&redirect_uri={your_redirect_uri}&response_type=code
        #Login to the URL and set the code returned by the login response in your Session object
        s.set_code (access_code)
        #Retrieve your access token
        access_token = s.retrieve_access_token()
        print ('Received access_token: %s' % access_token)
        
        self.u = Upstox (your_api_key, access_token)
        return self.u
    
    def subscribe_contracts(self,lstExch=['NSE_EQ','NSE_FO','NSE_INDEX']):
        prof=self.u.get_profile()
        lstExch_Enable=prof['exchanges_enabled']
        lstExch_Enable=lstExch
        for ex in lstExch_Enable:
            print(ex)
            self.u.get_master_contract(ex) 


    
    def nse_buy_intra(self,instrument,quantity,price):
        orderid=0
        try:
           print("nse_buy",instrument," ",quantity," ",price) 
           
           orderid=self.u.place_order(TransactionType.Buy,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Limit,  # order_type
                         ProductType.Intraday,  # product_type
                         price,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_buy function, place_order Runtime error: ",RuntimeError)
        except IOError :
            print (" nse_buy I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_buy Could not convert data to an integer.")
        except:
            print (" nse_buy Unexpected error:", sys.exc_info()[0])
            pass
        
        return orderid
    

    def nse_sell_intra(self,instrument,quantity,price):
        orderid=0

        try:
            
           print("nse_sell",instrument," ",quantity," ",price) 
           orderid=self.u.place_order(TransactionType.Sell,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Limit,  # order_type
                         ProductType.Intraday,  # product_type
                         price,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_sell function, place_order Runtime error: ",RuntimeError)
        except IOError:
            print (" nse_sell I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_sell Could not convert data to an integer.")
        except:
            print (" nse_sell Unexpected error:", sys.exc_info()[0])
            pass

        return orderid

    def nse_buy_del(self,instrument,quantity,price):
        orderid=0
        try:
           print("nse_buy_del",instrument," ",quantity," ",price) 
           
           self.u.place_order(TransactionType.Buy,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Limit,  # order_type
                         ProductType.Delivery,  # product_type
                         price,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_buy function, place_order Runtime error: ",RuntimeError)
        except IOError :
            print (" nse_buy I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_buy Could not convert data to an integer.")
        except:
            print (" nse_buy Unexpected error:", sys.exc_info()[0])
            pass
        
        return orderid
    
    def nse_sell_del(self,instrument,quantity,price):
        orderid=0
        try:
            
           print("nse_sell_del",instrument," ",quantity," ",price) 
           self.u.place_order(TransactionType.Sell,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Limit,  # order_type
                         ProductType.Delivery,  # product_type
                         price,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_sell function, place_order Runtime error: ",RuntimeError)
        except IOError:
            print (" nse_sell I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_sell Could not convert data to an integer.")
        except:
            print (" nse_sell Unexpected error:", sys.exc_info()[0])
            pass

        return orderid
    
    def nse_buy_del_market(self,instrument,quantity,price):
        orderid=0
        try:
           print("nse_buy_del_market",instrument," ",quantity," ",price) 
           
           orderid=self.u.place_order(TransactionType.Buy,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Market,  # order_type
                         ProductType.Delivery,  # product_type
                         0.0,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_buy function, place_order Runtime error: ",RuntimeError)
        except IOError :
            print (" nse_buy I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_buy Could not convert data to an integer.")
        except:
            print (" nse_buy Unexpected error:", sys.exc_info()[0])
            pass
        
        return orderid
    
    def nse_buy_intra_market(self,instrument,quantity,price):
        orderid=0
        try:
           print("nse_buy_intra_market",instrument," ",quantity," ",price) 
           
           self.u.place_order(TransactionType.Buy,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Market,  # order_type
                         ProductType.Intraday,  # product_type
                         0.0,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_buy function, place_order Runtime error: ",RuntimeError)
        except IOError :
            print (" nse_buy I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_buy Could not convert data to an integer.")
        except:
            print (" nse_buy Unexpected error:", sys.exc_info()[0])
            pass

        return orderid
    
    def nse_sell_intra_market(self,instrument,quantity,price):
        orderid=0
        try:
           print("nse_sell_intra_market",instrument," ",quantity," ",price) 
           
           self.u.place_order(TransactionType.Sell,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Market,  # order_type
                         ProductType.Intraday,  # product_type
                         0.0,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_sell_intra_market function, place_order Runtime error: ",RuntimeError)
        except IOError :
            print (" nse_sell_intra_market I/O error({0})".format(IOError))
        except ValueError:
            print ("nse_sell_intra_market Could not convert data to an integer.")
        except:
            print ("nse_sell_intra_market Unexpected error:", sys.exc_info()[0])
            pass
        return orderid
    
    def nse_buy_intra_better(self,instrument,quantity,price,wait_sec):
        orderid=self.nse_buy_intra(instrument,quantity,price)
        time.sleep(wait_sec)
        if orderid!=0:        
            df_order=json_normalize(self.u.get_order_history())
            
            df_order=df_order[df_order['order_id']==int(orderid['order_id'])]
            if df_order['status'].iloc[0]=='rejected':
                pass
            else:
                exec_quant=int(df_order['traded_quantity'].sum())
                if exec_quant!=quantity:
                    self.u.cancel_order(int(orderid['order_id']))
                    exec_quant=int(df_order['traded_quantity'].sum())

                    orderid=self.nse_buy_intra_market(instrument,int(quantity-exec_quant),price)

    def nse_sell_intra_better(self,instrument,quantity,price,wait_sec):
        orderid=self.nse_sell_intra(instrument,quantity,price)
        time.sleep(wait_sec)
        if orderid!=0:        
            df_order=json_normalize(self.u.get_order_history())
            
            df_order=df_order[df_order['order_id']==int(orderid['order_id'])]
            if df_order['status'].iloc[0]=='rejected':
                pass
            else:
                exec_quant=int(df_order['traded_quantity'].sum())
                if exec_quant!=quantity:
                    self.u.cancel_order(int(orderid['order_id']))
                    exec_quant=int(df_order['traded_quantity'].sum())

                    orderid=self.nse_sell_intra_market(instrument,int(quantity-exec_quant),price)


    def nse_sell_del_market(self,instrument,quantity,price):
        orderid=0
        
        try:
           
           print("nse_sell_del_market",instrument," ",quantity," ",price) 
           self.u.place_order(TransactionType.Sell,  # transaction_type
                         instrument,  # instrument
                         quantity,  # quantity
                         OrderType.Market,  # order_type
                         ProductType.Delivery,  # product_type
                         0.0,  # price
                         None,  # trigger_price
                         0,  # disclosed_quantity
                         DurationType.DAY,  # duration
                         None,  # stop_loss
                         None,  # square_off
                         None)  # trailing_ticks
           
        except (RuntimeError):
            print ("nse_sell function, place_order Runtime error: ",RuntimeError)
        except IOError:
            print (" nse_sell I/O error({0})".format(IOError))
        except ValueError:
            print (" nse_sell Could not convert data to an integer.")
        except:
            print (" nse_sell Unexpected error:", sys.exc_info()[0])
            pass
        
        return orderid

    
    def nse_buy_oco(self,instrument,quantity,price,sl,tr):
        self.u.place_order(TransactionType.Buy,  # transaction_type
                 #self.u.get_instrument_by_symbol('NSE_EQ', 'RELIANCE'),  # instrument
                 instrument,
                 quantity,  # quantity
                 OrderType.Limit,  # order_type
                 ProductType.OneCancelsOther,  # product_type
                 price,  # price
                 None,  # trigger_price
                 0,  # disclosed_quantity
                 DurationType.DAY,  # duration
                 sl,  # stop_loss
                 tr,  # square_off
                 30)  # trailing_ticks 20 * 0.05

    def nse_sell_oco(self,instrument,quantity,price,sl,tr):
        self.u.place_order(TransactionType.Sell,  # transaction_type
                 #self.u.get_instrument_by_symbol('NSE_EQ', 'RELIANCE'),  # instrument
                 instrument,
                 quantity,  # quantity
                 OrderType.Limit,  # order_type
                 ProductType.OneCancelsOther,  # product_type
                 price,  # price
                 None,  # trigger_price
                 0,  # disclosed_quantity
                 DurationType.DAY,  # duration
                 sl,  # stop_loss
                 tr,  # square_off
                 30)  # trailing_ticks 20 * 0.05


    def positions_df(self,position_type='D'):
        try:
            positions=self.u.get_positions()
            if len(positions)>0:
                df_pos_temp=json_normalize(positions)
                if (position_type=='D')|(position_type=='I'):
                    df_positions=df_pos_temp[df_pos_temp['product']==position_type].copy()
                else:
                    df_positions=df_pos_temp.copy()
                return df_positions.reset_index(drop=True,inplace=True)
            else:
                pd.DataFrame()
        except (RuntimeError, TypeError, NameError,requests.HTTPError) as e:
            error_in_fetching_bal=True
            print("error in fetching balances and positions",e)

        
    def register_instruments(self,script_cat,script):
        flag_success=True

        try:
            inst=self.u.get_instrument_by_symbol(script_cat, script)
            self.u.subscribe(inst, LiveFeedType.Full)
        except (RuntimeError, TypeError, NameError,requests.HTTPError) as e:
            print ("register_instruments function: ",e)
            flag_success=False
        except:
            print("error in fetching script "+script)     
            flag_success=False
        
        return flag_success,inst

    def unsubscribe_instruments(self,inst):
        self.u.unsubscribe(inst, LiveFeedType.Full)
        
    '''
    def cancel_orders_inst(self,inst,script='',script_cat=''):
        self.u.get_order_history()
    '''


'''
u=cu.Connect()        
cu.subscribe_contracts()


            nfo=json_normalize(cu.u.get_master_contract('NSE_FO') ).T
            nsei=json_normalize(cu.u.get_master_contract('NSE_INDEX') ).T
nfo=json_normalize(cu.u.get_master_contract('NSE_FO') )
df=json_normalize(cu.u.get_order_history())
'''
#cu=Connect_broker('sunil')
#u=cu.Connect()        



   
   
